"""Real HTTP/RocksDB checks for partition reads, replay and idempotent ACKs."""

import pickle
import random
import base64
import threading
import importlib.util
import os
from tempfile import TemporaryDirectory
from pathlib import Path
from threading import Thread
import unittest
from unittest.mock import patch
from waitress import wasyncore
import requests
from stopover import Stopover, Response, errors, compress, decompress, pack, unpack
from stopover_server.broker import Broker
from stopover_server.partition import Partition, PartitionItem
from stopover_server.server import create_server


class BrokerFixture(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory(prefix='stopover-delivery-')
        self.addCleanup(self.directory.cleanup)
        config = {'global': {'data_dir': self.directory.name, 'partitions': 2,
                            'rebalance_interval': 3600, 'receiver_timeout': 60,
                             'prune_interval': 3600, 'ttl': 86400}, 'streams': {}}
        config['global'].update(getattr(self, 'config_overrides', {}))
        # Rebalance deterministically; production broker/partition/HTTP code is real.
        with patch('stopover_server.broker.Thread.start'):
            self.broker = Broker(config)
        self.addCleanup(self.close_stores)
        self.server = create_server(self.broker, host='127.0.0.1', port=0,
                                    asyncore_loop_timeout=.02)
        self.server.server_port = self.server.effective_port
        self.server.get_app = lambda: self.server.application
        self.server.set_app = lambda app: setattr(self.server, 'application', app)
        self.thread = Thread(target=self.server.run, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)
        self.endpoint = f'http://127.0.0.1:{self.server.server_port}'
        random.seed(0)

    def stop_server(self):
        self.server.task_dispatcher.shutdown()
        wasyncore.close_all(map=self.server._map)
        self.thread.join(2)
        self.assertFalse(self.thread.is_alive())

    def close_stores(self):
        for partitions in self.broker.partitions.values():
            for partition in partitions.values():
                partition._store.close()

    def rpc(self, method, **params):
        response = requests.post(self.endpoint, data=compress(pack({'method': method, 'params': params})), timeout=3)
        response.raise_for_status()
        return Response(**unpack(decompress(response.content)))

    def publish(self, value, partition=0, stream='intake'):
        if not isinstance(value, (str, int, float, bytes, bool)):
            value = pickle.dumps(value, protocol=5)
        response = self.rpc('put_message', value=value, stream=stream, partition=partition)
        self.assertEqual(response.status, 20)
        return response

    def check_stream_traversal_and_invalid_partition_types(self):
        outside = Path(self.directory.name) / 'outside'
        outside.mkdir()
        (outside / 'preserved').write_text('original')
        for method in ('put_message', 'get_message', 'get_partitions', 'commit_message', 'set_offset'):
            for stream in ('../outside', '.', '..', '/absolute', 'nested/topic', 'nested\\topic', 'bad\nname', '', None, 1):
                with self.subTest(method=method, stream=stream):
                    params = {'stream': stream, 'partition': 0, 'index': 0,
                              'receiver_group': 'test', 'receiver': 'fixture', 'value': 'test'}
                    response = requests.post(self.endpoint, data=compress(pack({'method': method, 'params': params})), timeout=3)
                    self.assertEqual(response.status_code, 400)
        for partition in ('../outside', '0', -1, 1.0, True):
            response = requests.post(self.endpoint, json={'method': 'set_offset', 'params': {
                'stream': 'safe', 'partition': partition, 'index': 0, 'receiver_group': 'test'}}, timeout=3)
            self.assertEqual(response.status_code, 400)
        self.assertEqual(list(outside.iterdir()), [outside / 'preserved'])
        self.assertEqual(self.broker.partitions, {})

    def check_existing_stream_and_partition_links(self):
        root = Path(self.directory.name)
        outside = root / 'outside'
        outside.mkdir()
        streams = root / 'streams'
        streams.mkdir()
        (streams / 'linked').symlink_to(outside, target_is_directory=True)
        response = requests.post(self.endpoint, json={'method': 'put_message', 'params': {
            'stream': 'linked', 'value': 'test'}}, timeout=3)
        self.assertEqual(response.status_code, 400)
        (streams / 'safe').mkdir()
        (streams / 'safe' / '0').symlink_to(outside, target_is_directory=True)
        with self.assertRaises(ValueError):
            Partition('safe', 0, self.directory.name, create_if_missing=True)
        self.assertEqual(list(outside.iterdir()), [])
        valid = Partition('safe', 1, self.directory.name, create_if_missing=True)
        valid._store.close()
        with patch('stopover_server.broker.time.sleep', side_effect=[None, RuntimeError('stop-fixture')]), \
                patch.object(Partition, 'prune', autospec=True) as prune:
            with self.assertRaisesRegex(RuntimeError, 'stop-fixture'):
                self.broker._prune_loop()
            prune.assert_called_once()
            self.assertEqual(prune.call_args.args[0].number, 1)

    def assign(self, receiver, group='test', stream='intake'):
        self.broker.knock({'receiver_group': group, 'receiver': receiver}, do_log=False)
        self.broker._get_receiver_partition_numbers(stream, group, receiver)
        self.broker._rebalance()

    def client(self, uid='worker', group='test'):
        client = Stopover(self.endpoint, uid=uid)
        self.addCleanup(client.session.close)
        self.assign(uid, group)
        return client


class StorageBoundaryTests(BrokerFixture):
    test_stream_traversal = BrokerFixture.check_stream_traversal_and_invalid_partition_types
    test_existing_links = BrokerFixture.check_existing_stream_and_partition_links


class DeliveryTests(BrokerFixture):
    def test_heartbeat_logs_at_debug(self):
        with self.assertLogs(level='DEBUG') as captured:
            response = self.broker.knock({'receiver_group': 'test', 'receiver': 'worker'})
        self.assertEqual(response['status'], 20)
        self.assertEqual([record.levelname for record in captured.records], ['DEBUG'])
        self.assertIn('test/worker is knocking', captured.output[0])

    def test_assignments_log_only_when_the_result_changes(self):
        with self.assertLogs(level='INFO') as initial:
            self.assign('first')
        self.assertTrue(any('intake.test.first=[0, 1]' in line for line in initial.output))
        with self.assertNoLogs(level='INFO'):
            self.broker._rebalance()
            self.broker._rebalance()
        with self.assertLogs(level='INFO') as joined:
            self.assign('second')
        self.assertTrue(any('intake.test.second=[1]' in line for line in joined.output))
        with self.assertNoLogs(level='INFO'):
            self.broker._rebalance()
        self.broker.last_seen_by_group['test']['first'] = 0
        with self.assertLogs(level='INFO') as removed:
            self.broker._rebalance()
        self.assertTrue(any('receiver "first" kicked' in line for line in removed.output))
        self.assertTrue(any('intake.test.second=[0, 1]' in line for line in removed.output))
        self.assertFalse(any('intake.test.first=' in line for line in removed.output))
        with self.assertNoLogs(level='INFO'):
            self.broker._rebalance()

    def test_binary_length_prefix_is_not_confused_with_json(self):
        for length in range(1, 200):
            data = {'method': 'get_message', 'params': {
                'stream': 'intake', 'receiver_group': 'test', 'receiver': 'r' * length,
                'partition': None, 'index': None, 'skip_pruned': True,
            }}
            packed = compress(pack(data))
            if packed[:1] == b'{':
                break
        else:
            self.fail('Could not construct the Snappy/JSON prefix collision')
        response = requests.post(self.endpoint, data=packed, timeout=2)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(unpack(decompress(response.content))['status'], 22)
        plain = requests.post(self.endpoint, json=data, timeout=2)
        self.assertEqual(plain.status_code, 200)
        self.assertEqual(plain.json()['status'], 22)
        for malformed in (b'{invalid', b'garbage', b'[]', b'{"method":"knock"}'):
            self.assertEqual(requests.post(self.endpoint, data=malformed, timeout=2).status_code, 400)

    def test_progress_skips_pruned_prefix_but_explicit_reads_remain_exact(self):
        self.broker.config['global']['partitions'] = 1
        with patch('stopover_server.utils.get_timestamp_ms', return_value=1000):
            self.publish('first')
            self.publish('expired')
        client = self.client()
        first = client.get('intake', 'test', progress_without_commit=True)
        with patch('stopover_server.utils.get_timestamp_ms', return_value=10000):
            self.publish('fresh')
            self.broker._get_partition('intake', 0).prune(5)
        with self.assertRaises(errors.GetError):
            client.get('intake', 'test', partition=0, index=1)
        fresh = client.get('intake', 'test', progress_without_commit=True)
        self.assertEqual((fresh.index, fresh.value), (2, 'fresh'))
        partition = self.broker._get_partition('intake', 0)
        self.assertEqual(partition._get_offset('test'), -1)
        client.commit(first, 'test')
        client.commit(fresh, 'test')
        self.assertEqual(partition._get_offset('test'), 2)
        with patch('stopover_server.utils.get_timestamp_ms', return_value=20000):
            partition.prune(5)
        with self.assertRaises(errors.GetError):
            client.get('intake', 'test', progress_without_commit=True)
        self.publish('after-empty')
        following = client.get('intake', 'test', progress_without_commit=True)
        self.assertEqual((following.index, following.value), (3, 'after-empty'))
        client.commit(following, 'test')

    def test_auto_commit_retries_the_same_message_before_or_after_a_lost_ack(self):
        self.broker.config['global']['partitions'] = 1
        self.publish('first')
        self.publish('second')
        for accepted in (False, True):
            group = 'auto-' + str(accepted)
            client = self.client(uid=group, group=group)
            commit = client._commit_call

            def fail(message, receiver_group):
                if accepted:
                    commit(message, receiver_group)
                raise requests.ConnectionError('response lost')

            with patch.object(client, '_commit_call', side_effect=fail):
                with self.assertRaises(errors.ServerConnectionError):
                    client.get('intake', group, progress_without_commit=True, auto_commit=True)
            self.assertEqual(client.temporary_offsets['intake'][group][group], {0: 0})
            first = client.get('intake', group, progress_without_commit=True, auto_commit=True)
            second = client.get('intake', group, progress_without_commit=True, auto_commit=True)
            self.assertEqual((first.index, first.value), (0, 'first'))
            self.assertEqual((second.index, second.value), (1, 'second'))

    def test_reconnecting_receiver_retains_its_heartbeat_after_expiry(self):
        client = self.client()
        self.broker.last_seen_by_group['test']['worker'] = 0
        expiry_decided, attempted, release = (threading.Event() for _ in range(3))
        parts, knock = self.broker._get_stream_partition_numbers, self.broker.knock
        failures = []

        def pause_expiry(stream):
            if threading.current_thread().name == 'expiry-test':
                expiry_decided.set()
                if not release.wait(3):
                    raise AssertionError('barrier timed out')
            return parts(stream)

        def reconnect():
            try:
                client.get('intake', 'test')
            except errors.GetError as error:
                failures.append(error.args[0])

        def observe_knock(params, do_log=True):
            attempted.set()
            return knock(params, do_log=do_log)

        with patch.object(self.broker, '_get_stream_partition_numbers', side_effect=pause_expiry), \
                patch.object(self.broker, 'knock', side_effect=observe_knock):
            expiry = threading.Thread(target=self.broker._rebalance, name='expiry-test')
            expiry.start()
            self.assertTrue(expiry_decided.wait(2))
            reader = threading.Thread(target=reconnect)
            reader.start()
            try:
                self.assertTrue(attempted.wait(2))
            finally:
                release.set()
            expiry.join(2)
            reader.join(2)
        self.assertFalse(expiry.is_alive())
        self.assertFalse(reader.is_alive())
        self.assertEqual(failures, [22])
        self.assertIn('worker', self.broker.last_seen_by_group['test'])
        self.broker._rebalance()
        self.broker._rebalance()
        self.assertEqual(self.broker._get_receiver_partition_numbers('intake', 'test', 'worker'), [0, 1])
        self.publish('still-consumable')
        self.assertEqual(client.get('intake', 'test').value, 'still-consumable')

    def test_basic_preserves_colons_and_rejects_malformed_headers(self):
        self.broker.config['auth'] = {'fixture': 'left:middle:right'}
        client = Stopover(self.endpoint, client_id='fixture', client_secret='left:middle:right')
        self.addCleanup(client.session.close)
        client.put('accepted', 'intake')
        data = compress(pack({'method': 'knock', 'params': {'receiver_group': 'test', 'receiver': 'worker'}}))
        valid = base64.b64encode(b'fixture:left:middle:right').decode()
        response = requests.post(self.endpoint, data=data, headers={'Authorization': 'basic ' + valid}, timeout=2)
        self.assertEqual(response.status_code, 200)
        for value in ('Basic !!!', 'Basic ' + base64.b64encode(b'missing-delimiter').decode(), 'Bearer token'):
            with self.subTest(header=value):
                response = requests.post(self.endpoint, data=data, headers={'Authorization': value}, timeout=2)
                self.assertEqual(response.status_code, 403)

    def test_original_partition_data_can_be_read_written_and_reopened_by_the_old_reader(self):
        source = os.environ.get('STOPOVER_LEGACY_PARTITION',
                                os.path.join(os.path.dirname(__file__),
                                             'fixtures/legacy_partition.py'))
        spec = importlib.util.spec_from_file_location('stopover_server.legacy_partition', source)
        legacy = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(legacy)
        old = legacy.Partition('compatibility', 0, self.directory.name, create_if_missing=True)
        try:
            old.put(legacy.PartitionItem(b'historical-payload', timestamp=1))
        finally:
            old._store.close()
        current = Partition('compatibility', 0, self.directory.name)
        try:
            self.assertEqual(current.get('reader')['value'], b'historical-payload')
            current.put(PartitionItem(b'new-payload', timestamp=2))
            current.commit(0, 'reader')
            current.commit(0, 'reader')
        finally:
            current._store.close()
        old = legacy.Partition('compatibility', 0, self.directory.name)
        try:
            self.assertEqual(old._get_offset('reader'), 0)
            self.assertEqual(old.get('reader')['value'], b'new-payload')
            self.assertEqual(old.get('reader', 0)['value'], b'historical-payload')
        finally:
            old._store.close()

    def test_explicit_partition_and_temporary_offsets_never_cross_partitions(self):
        expected = {(partition, index) for partition, size in ((0, 3), (1, 2)) for index in range(size)}
        for partition, index in sorted(expected):
            self.publish(f'{partition}:{index}', partition)
        client = self.client()
        explicit = client.get('intake', 'test', partition=1, index=0)
        self.assertEqual((explicit.partition, explicit.index, explicit.value), (1, 0, '1:0'))
        received = []
        for _ in range(100):
            try:
                message = client.get('intake', 'test', progress_without_commit=True)
            except errors.GetError:
                continue
            self.assertEqual(set(message.assigned_partitions), {0, 1})
            received.append((message.partition, message.index))
            if len(received) == len(expected):
                break
        self.assertEqual(set(received), expected)
        self.assertEqual(len(received), len(expected))
        for partition in (0, 1):
            self.assertEqual(self.broker._get_partition('intake', partition)._get_offset('test'), -1)
        restarted = Stopover(self.endpoint, uid='worker')
        self.addCleanup(restarted.session.close)
        first = restarted.get('intake', 'test', progress_without_commit=True)
        self.assertEqual(first.index, 0)

    def test_rebalance_and_end_of_stream_refresh_the_assigned_partitions(self):
        self.publish('a', 0)
        self.publish('b', 1)
        client = self.client()
        with patch('stopover.stopover.random.randint', return_value=0):
            client.get('intake', 'test', progress_without_commit=True)
        self.client(uid='second')
        self.assertEqual(self.broker._get_receiver_partition_numbers('intake', 'test', 'worker'), [0])
        with patch.object(client, '_get_next_partition_and_index', return_value=(1, 0)):
            with self.assertRaises(errors.GetError):
                client.get('intake', 'test', progress_without_commit=True)
        self.assertEqual(client.temporary_offsets['intake']['test']['worker'], {0: 1})
        with self.assertRaises(errors.GetError):
            client.get('intake', 'test', progress_without_commit=True)
        self.assertEqual(client.temporary_offsets['intake']['test']['worker'], {0: 1})

    def test_ack_retries_are_idempotent_but_gaps_and_invalid_offsets_are_rejected(self):
        for value in ('a', 'b', 'c'):
            self.publish(value)
        client = self.client()
        messages = [client.get('intake', 'test', partition=0, index=index) for index in range(3)]
        with self.assertRaises(errors.CommitError):
            client.commit(messages[1], 'test')
        client.commit(messages[0], 'test')
        client.commit(messages[0], 'test')
        partition = self.broker._get_partition('intake', 0)
        self.assertEqual(partition._get_offset('test'), 0)
        for offset in (-1, True, 0.5, 3):
            with self.subTest(offset=offset):
                reply = self.rpc('commit_message', stream='intake', receiver_group='test', partition=0, index=offset)
                self.assertEqual(reply.status, 50)
                self.assertEqual(partition._get_offset('test'), 0)
        client.commit(messages[1], 'test')
        partition._store.close()
        reopened = Partition('intake', 0, self.directory.name)
        self.broker.partitions['intake'][0] = reopened
        self.assertEqual(reopened._get_offset('test'), 1)
        self.assertEqual(reopened.get('test')['value'], 'c')
        self.assertEqual(reopened._get_message_key(2), b'\x00' + (2).to_bytes(8, 'big'))


if __name__ == '__main__':
    unittest.main()
