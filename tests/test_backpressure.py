"""Bounded real HTTP queues; overload must never execute a rejected operation."""
from concurrent.futures import ThreadPoolExecutor
import json
import socket
from threading import Event
import time
from unittest.mock import patch

import requests
from stopover import errors
from stopover_server import utils
from stopover_server.server import StreamDispatcher, create_server
from test_delivery import BrokerFixture


class BackpressureTests(BrokerFixture):
    config_overrides = {'partitions': 1, 'max_pending_requests': 8,
                        'max_stream_pending_requests': 4, 'max_pending_bytes': 32768,
                        'max_stream_pending_bytes': 16384, 'max_request_bytes': 4096}

    def setUp(self):
        super().setUp()
        self.release = Event()
        self.pool = ThreadPoolExecutor(max_workers=12)
        self.addCleanup(self.close_workers)

    def close_workers(self):
        self.release.set()
        self.pool.shutdown(wait=True)

    def rpc_json(self, method, **params):
        with requests.post(self.endpoint, json={'method': method, 'params': params}, timeout=3) as response:
            data = response.json() if response.content and response.headers.get('Content-Type', '').startswith('application/json') else None
            return response.status_code, data, response.headers

    def wait_count(self, count, stream=None):
        dispatcher = self.server.task_dispatcher
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            with dispatcher.lock:
                actual = len(dispatcher.streams.get(stream, ())) if stream else dispatcher.pending_requests
                if actual == count:
                    if not count and stream is None:
                        self.assertEqual(dispatcher.pending_bytes, 0)
                        self.assertFalse(dispatcher.stream_bytes)
                    return
            time.sleep(.005)
        self.fail(f'Pending count stayed at {actual}, expected {count}')

    def blocked_put(self, params):
        if params['stream'].startswith('busy') and not self.release.wait(5):
            raise AssertionError('Fixture barrier timed out')
        return self.real_put(params)

    def expect_overload(self, reply):
        status, data, headers = reply
        self.assertEqual(status, 503)
        self.assertEqual(headers.get('Retry-After'), '1')
        self.assertEqual(headers.get('Connection'), 'close')

    def test_stream_count_limit_preserves_fifo_and_rejected_put_can_be_retried(self):
        self.real_put = self.broker.put_message
        with patch.object(self.broker, 'put_message', side_effect=self.blocked_put):
            futures = []
            try:
                for number in range(4):
                    futures.append(self.pool.submit(self.rpc_json, 'put_message', stream='busy', value=number))
                    self.wait_count(number + 1, 'busy')
                self.expect_overload(self.rpc_json('put_message', stream='busy', value=4))
                self.assertEqual(self.rpc_json('put_message', stream='free', value='free')[1]['index'], 0)
            finally:
                self.release.set()
            self.assertEqual([future.result(timeout=3)[1]['index'] for future in futures], list(range(4)))
        self.wait_count(0)
        partition = self.broker._get_partition('busy', 0)
        self.assertEqual(partition._get_index(), 3)
        self.assertEqual([partition.get('inspect', i)['value'] for i in range(4)], list(range(4)))
        self.assertEqual(self.rpc_json('put_message', stream='busy', value=4)[1]['index'], 4)
        self.wait_count(0)

    def test_global_count_limit_rejects_even_when_all_workers_are_busy(self):
        self.real_put = self.broker.put_message
        with patch.object(self.broker, 'put_message', side_effect=self.blocked_put):
            futures = []
            try:
                for number in range(8):
                    futures.append(self.pool.submit(self.rpc_json, 'put_message', stream=f'busy-{number % 4}', value=number))
                    self.wait_count(number + 1)
                for _ in range(12):
                    self.expect_overload(self.rpc_json('put_message', stream='rejected', value='never'))
                self.expect_overload(self.rpc_json('health'))
                self.assertNotIn('rejected', self.broker.partitions)
                self.wait_count(8)
            finally:
                self.release.set()
            self.assertTrue(all(future.result(timeout=3)[0] == 200 for future in futures))
        self.wait_count(0)
        self.assertEqual(self.rpc_json('health')[0], 200)

    def test_stream_byte_limit_rejects_below_the_request_count_limit(self):
        self.real_put = self.broker.put_message
        with patch.object(self.broker, 'put_message', side_effect=self.blocked_put):
            futures = []
            try:
                for number in range(2):
                    futures.append(self.pool.submit(self.rpc_json, 'put_message', stream='busy', value='x' * 3500))
                    self.wait_count(number + 1, 'busy')
                self.expect_overload(self.rpc_json('put_message', stream='busy', value='x' * 3500))
                self.wait_count(2, 'busy')
                self.assertLess(self.server.task_dispatcher.pending_requests, self.config_overrides['max_stream_pending_requests'])
                self.assertEqual(self.rpc_json('put_message', stream='free', value='small')[0], 200)
            finally:
                self.release.set()
            for future in futures:
                self.assertEqual(future.result(timeout=3)[0], 200)
        self.wait_count(0)

    def test_global_byte_limit_counts_raw_and_decoded_payloads(self):
        self.real_put = self.broker.put_message
        with patch.object(self.broker, 'put_message', side_effect=self.blocked_put):
            futures = []
            try:
                for number in range(4):
                    futures.append(self.pool.submit(self.rpc_json, 'put_message', stream=f'busy-{number}', value='x' * 3500))
                    self.wait_count(1, f'busy-{number}')
                self.expect_overload(self.rpc_json('put_message', stream='rejected', value='x' * 3500))
                self.wait_count(4)
                self.assertLessEqual(self.server.task_dispatcher.pending_bytes, self.config_overrides['max_pending_bytes'])
                self.assertNotIn('rejected', self.broker.partitions)
            finally:
                self.release.set()
            for future in futures:
                self.assertEqual(future.result(timeout=3)[0], 200)
        self.wait_count(0)

    def test_oversized_snappy_is_rejected_before_decompression(self):
        data = {'method': 'put_message', 'params': {'stream': 'large', 'value': b'x' * 32768}}
        compressed = utils.compress(utils.pack(data))
        self.assertLess(len(compressed), self.config_overrides['max_request_bytes'])
        with patch('stopover_server.utils.decompress', side_effect=AssertionError('must not decompress')) as decompress:
            for payload in (compressed, b'\xff\xff\xff\xff\x0f'):
                with requests.post(self.endpoint, data=payload, timeout=2) as response:
                    self.assertEqual(response.status_code, 413)
            decompress.assert_not_called()
        self.wait_count(0)
        self.assertNotIn('large', self.broker.partitions)

    def test_oversized_json_is_rejected_and_json_fallback_remains_compatible(self):
        with requests.post(self.endpoint, json={'method': 'put_message', 'params': {'stream': 'large', 'value': 'x' * 8192}}, timeout=2) as response:
            self.assertEqual(response.status_code, 413)
        self.assertNotIn('large', self.broker.partitions)
        for prefix in (b'', b'\xef\xbb\xbf'):
            with requests.post(self.endpoint, data=prefix + b'{"method":"health","params":{}}', timeout=2) as response:
                self.assertEqual(response.status_code, 200)
        self.wait_count(0)

    def test_rejected_ack_does_not_advance_offset_or_get_mistaken_for_success(self):
        self.publish('first')
        client = self.client()
        message = client.get('intake', 'test')
        get = self.broker.get_message

        def hold(params):
            if not self.release.wait(5):
                raise AssertionError('Fixture barrier timed out')
            return get(params)

        with patch.object(self.broker, 'get_message', side_effect=hold):
            futures = []
            try:
                for number in range(4):
                    futures.append(self.pool.submit(self.rpc_json, 'get_message', stream='intake', receiver_group='test', receiver='worker'))
                    self.wait_count(number + 1, 'intake')
                with self.assertRaises(errors.ServerConnectionError):
                    client.commit(message, 'test')
                self.assertEqual(self.broker._get_partition('intake', 0)._get_offset('test'), -1)
            finally:
                self.release.set()
            for future in futures:
                self.assertEqual(future.result(timeout=3)[1]['index'], 0)
        self.wait_count(0)
        client.commit(message, 'test')
        self.assertEqual(self.broker._get_partition('intake', 0)._get_offset('test'), 0)

    def test_failure_releases_the_entire_byte_budget(self):
        with patch.object(self.broker, 'put_message', side_effect=RuntimeError('fixture')):
            for _ in range(12):
                self.assertEqual(self.rpc_json('put_message', stream='failed', value='x' * 3500)[0], 500)
                self.wait_count(0)
        self.assertEqual(self.rpc_json('put_message', stream='recovered', value='x' * 3500)[1]['index'], 0)
        self.wait_count(0)

    def test_repeated_concurrent_bursts_keep_accounting_and_persist_only_successes(self):
        put = self.broker.put_message
        dispatcher = self.server.task_dispatcher

        def slower(params):
            with dispatcher.lock:
                self.assertLessEqual(dispatcher.pending_requests, self.config_overrides['max_pending_requests'])
                self.assertLessEqual(dispatcher.pending_bytes, self.config_overrides['max_pending_bytes'])
            time.sleep(.01)
            return put(params)

        def produce(producer):
            outcomes = []
            for number in range(24):
                stream, value = f'stream-{producer % 6}', f'{producer}:{number}'
                status, reply, headers = self.rpc_json('put_message', stream=stream, value=value)
                self.assertIn(status, (200, 503))
                if status == 200:
                    outcomes.append((stream, reply['index'], value))
                else:
                    self.assertEqual(headers.get('Retry-After'), '1')
            return outcomes

        with patch.object(self.broker, 'put_message', side_effect=slower):
            outcomes = [item for group in self.pool.map(produce, range(12)) for item in group]
        self.wait_count(0)
        self.assertEqual(len({(stream, index) for stream, index, value in outcomes}), len(outcomes))
        persisted = 0
        for stream, partitions in self.broker.partitions.items():
            partition = partitions[0]
            stored = {index: partition.get('inspect', index)['value'] for index in range(partition._get_index() + 1)}
            self.assertEqual(stored, {index: value for name, index, value in outcomes if name == stream})
            persisted += len(stored)
        self.assertEqual(persisted, len(outcomes))

    def test_all_limit_settings_require_positive_integers(self):
        for setting in StreamDispatcher.DEFAULT_LIMITS:
            for value in (0, -1, True, 1.5, '10', None):
                with self.subTest(setting=setting, value=value), patch.dict(self.broker.config['global'], {setting: value}):
                    with self.assertRaisesRegex(ValueError, setting):
                        create_server(self.broker, host='127.0.0.1', port=0)

    def test_pipelined_overload_preserves_first_response_and_drops_unaccepted_work(self):
        dispatcher = self.server.task_dispatcher
        dispatcher.limits['max_pending_requests'] = 1
        payload = b''
        for number in (0, 1):
            body = json.dumps({'method': 'put_message', 'params': {'stream': 'pipeline', 'value': number}}).encode()
            payload += (b'POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: '
                        + str(len(body)).encode() + b'\r\n\r\n' + body)
        with socket.create_connection(('127.0.0.1', self.server.server_port), timeout=3) as connection:
            connection.sendall(payload)
            with connection.makefile('rb') as reader:
                for status in (200, 503):
                    self.assertIn(str(status).encode(), reader.readline())
                    headers = {}
                    while (line := reader.readline()) != b'\r\n':
                        self.assertTrue(line)
                        key, value = line.split(b':', 1)
                        headers[key.lower()] = value.strip()
                    body = reader.read(int(headers[b'content-length']))
                    if status == 200:
                        self.assertEqual(json.loads(body)['index'], 0)
                    else:
                        self.assertEqual(headers[b'retry-after'], b'1')
                self.assertEqual(reader.read(1), b'')
        self.wait_count(0)
        self.assertEqual(self.broker._get_partition('pipeline', 0)._get_index(), 0)
