"""Real HTTP ordering, fairness, shutdown and initialization under concurrency."""
from concurrent.futures import ThreadPoolExecutor
import json
import socket
from threading import Event
import time
from unittest.mock import patch

import requests

from stopover_server import utils
from stopover_server.partition import Partition
from stopover_server.server import create_server
from test_delivery import BrokerFixture


class StreamThreadTests(BrokerFixture):
    def setUp(self):
        super().setUp()
        self.broker.config['global']['partitions'] = 1
        self.pool = ThreadPoolExecutor(max_workers=16)
        self.release = Event()
        self.addCleanup(self.close_workers)

    def close_workers(self):
        self.release.set()
        self.pool.shutdown(wait=True)

    def request(self, method, **params):
        with requests.post(self.endpoint, json={'method': method, 'params': params}, timeout=4) as response:
            return response.status_code, response.json() if response.content else None

    def wait_for_pending(self, stream, count):
        dispatcher = self.server.task_dispatcher
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            with dispatcher.lock:
                if len(dispatcher.streams.get(stream, ())) == count:
                    return
            time.sleep(.005)
        self.fail(f'{stream} did not reach {count} admitted requests')

    def test_fifo_backlog_does_not_occupy_workers_for_other_streams(self):
        entered = Event()
        order = []
        put = self.broker.put_message

        def hold(params):
            if params['stream'] == 'hot':
                order.append(params['value'])
                if params['value'] == 0:
                    entered.set()
                    if not self.release.wait(5):
                        raise AssertionError('Barrier timed out')
            return put(params)

        with patch.object(self.broker, 'put_message', side_effect=hold):
            first = self.pool.submit(self.request, 'put_message', stream='hot', value=0)
            self.assertTrue(entered.wait(2))
            pending = []
            try:
                for value in range(1, 9):
                    pending.append(self.pool.submit(self.request, 'put_message', stream='hot', value=value))
                    self.wait_for_pending('hot', value + 1)
                self.assertEqual(order, [0])
                cold = self.pool.submit(self.request, 'put_message', stream='cold', value='free')
                self.assertEqual(cold.result(timeout=2)[1]['index'], 0)
                self.assertEqual(self.request('health'), (200, {'status': 20}))
                self.assertEqual(self.request('knock', receiver_group='r', receiver='r')[1]['status'], 20)
                self.assertFalse(first.done())
            finally:
                self.release.set()
            replies = [first.result(timeout=3), *(future.result(timeout=3) for future in pending)]
        self.assertEqual(order, list(range(9)))
        self.assertEqual([reply[1]['index'] for reply in replies], list(range(9)))
        self.wait_for_pending('hot', 0)

    def test_partitions_of_the_same_stream_share_a_lane(self):
        self.broker.config['streams']['split'] = {'partitions': 2}
        entered = Event()
        put = self.broker.put_message

        def hold(params):
            if params['stream'] == 'split' and params['partition'] == 0:
                entered.set()
                if not self.release.wait(5):
                    raise AssertionError('Barrier timed out')
            return put(params)

        with patch.object(self.broker, 'put_message', side_effect=hold):
            first = self.pool.submit(self.request, 'put_message', stream='split', partition=0, value='a')
            self.assertTrue(entered.wait(2))
            second = self.pool.submit(self.request, 'put_message', stream='split', partition=1, value='b')
            try:
                self.wait_for_pending('split', 2)
                self.assertFalse(second.done())
                self.assertEqual(self.request('put_message', stream='other', value='c')[1]['status'], 20)
            finally:
                self.release.set()
            self.assertEqual(first.result(timeout=3)[1]['index'], 0)
            self.assertEqual(second.result(timeout=3)[1]['index'], 0)

    def test_reads_acks_and_offset_changes_follow_the_stream_queue(self):
        self.publish('a')
        self.publish('b')
        self.assign('worker')
        params = {'stream': 'intake', 'receiver_group': 'test', 'receiver': 'worker', 'partition': 0}
        entered = Event()
        get = self.broker.get_message
        count = 0

        def hold(data):
            nonlocal count
            count += 1
            if count == 1:
                entered.set()
                if not self.release.wait(5):
                    raise AssertionError('Barrier timed out')
            return get(data)

        with patch.object(self.broker, 'get_message', side_effect=hold):
            first = self.pool.submit(self.request, 'get_message', **params)
            self.assertTrue(entered.wait(2))
            operations = [('commit_message', {**params, 'index': 0}), ('get_message', params),
                          ('set_offset', {**params, 'index': -1}), ('get_message', params)]
            pending = []
            try:
                for number, (method, values) in enumerate(operations, 2):
                    pending.append(self.pool.submit(self.request, method, **values))
                    self.wait_for_pending('intake', number)
            finally:
                self.release.set()
            self.assertEqual(first.result(timeout=3)[1]['index'], 0)
            replies = [future.result(timeout=3) for future in pending]
            self.assertTrue(all(status == 200 and data['status'] == 20 for status, data in replies))
            self.assertEqual([replies[1][1]['index'], replies[3][1]['index']], [1, 0])

    def test_failed_handler_releases_its_stream(self):
        put = self.broker.put_message

        def fail_once(params):
            if params['value'] == 0:
                raise RuntimeError('fixture')
            return put(params)

        with patch.object(self.broker, 'put_message', side_effect=fail_once):
            self.assertEqual(self.request('put_message', stream='failure', value=0)[0], 500)
            self.assertEqual(self.request('put_message', stream='failure', value=1)[1]['status'], 20)
        self.assertEqual(self.broker._get_partition('failure', 0).get('reader')['value'], 1)
        self.wait_for_pending('failure', 0)

    def test_invalid_methods_and_payloads_do_not_stop_admission(self):
        for method in ([], {}, None, True, 'missing'):
            with self.subTest(method=method):
                self.assertEqual(self.request(method, stream='invalid')[0], 400)
        for payload in (b'{bad', b'\xff', b'[]'):
            with requests.post(self.endpoint, data=payload, timeout=2) as response:
                self.assertEqual(response.status_code, 400)
        self.assertEqual(self.request('put_message', stream='valid', value='ok')[1]['status'], 20)

    def test_auth_precedes_decoding_and_each_valid_body_is_decoded_once(self):
        self.broker.config['auth'] = {'fixture': 'secret'}
        decode = utils.decode_request
        with patch('stopover_server.utils.decode_request', wraps=decode) as called:
            with requests.post(self.endpoint, data=b'invalid', timeout=2) as response:
                self.assertEqual(response.status_code, 401)
            called.assert_not_called()
            with requests.post(self.endpoint, json={'method': 'health', 'params': {}},
                               auth=('fixture', 'secret'), timeout=2) as response:
                self.assertEqual(response.json()['status'], 20)
            self.assertEqual(called.call_count, 1)

    def test_http_pipeline_reclassifies_each_request_and_keeps_response_order(self):
        messages = [('first', 'a'), ('second', 'b'), ('first', 'c')]
        payload = b''
        for stream, value in messages:
            body = json.dumps({'method': 'put_message', 'params': {'stream': stream, 'value': value}}).encode()
            payload += (b'POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: '
                        + str(len(body)).encode() + b'\r\n\r\n' + body)
        with socket.create_connection(('127.0.0.1', self.server.server_port), timeout=3) as connection:
            connection.sendall(payload)
            with connection.makefile('rb') as reader:
                replies = []
                for _ in messages:
                    self.assertIn(b' 200 ', reader.readline())
                    headers = {}
                    while True:
                        line = reader.readline()
                        if line == b'\r\n':
                            break
                        name, value = line.split(b':', 1)
                        headers[name.lower()] = value.strip()
                    replies.append(json.loads(reader.read(int(headers[b'content-length']))))
        self.assertEqual([(row['stream'], row['index']) for row in replies], [('first', 0), ('second', 0), ('first', 1)])

    def test_initialization_does_not_publish_partial_partitions_or_open_twice(self):
        entered = Event()
        construct = Partition
        calls = []

        def hold(**params):
            calls.append((params['stream'], params['number']))
            entered.set()
            if not self.release.wait(5):
                raise AssertionError('Barrier timed out')
            return construct(**params)

        with patch('stopover_server.broker.Partition', side_effect=hold):
            first = self.pool.submit(self.broker._get_stream_partition_numbers, 'new')
            self.assertTrue(entered.wait(2))
            second = self.pool.submit(self.broker._get_stream_partition_numbers, 'new')
            try:
                self.assertNotIn('new', self.broker.partitions_by_stream)
                self.assertFalse(second.done())
            finally:
                self.release.set()
            self.assertEqual(first.result(timeout=3), [0])
            self.assertEqual(second.result(timeout=3), [0])
        self.assertEqual(calls, [('new', 0)])
        self.assertIs(self.broker._get_partition('new', 0), self.broker.partitions['new'][0])
        numbers = self.broker._get_stream_partition_numbers('new')
        numbers.clear()
        self.assertEqual(self.broker._get_stream_partition_numbers('new'), [0])

    def test_worker_count_validation(self):
        for threads in (0, -1, True, 1.5, '4', None):
            with self.subTest(threads=threads), self.assertRaises(ValueError):
                create_server(self.broker, threads=threads, host='127.0.0.1', port=0)
        server = create_server(self.broker, threads=1, host='127.0.0.1', port=0)
        try:
            self.assertEqual(len(server.task_dispatcher.threads), 1)
        finally:
            server.task_dispatcher.shutdown()
            server.close()

    def test_disconnected_client_does_not_strand_following_requests(self):
        entered = Event()
        put = self.broker.put_message

        def hold(params):
            if params['value'] == 'disconnected':
                entered.set()
                if not self.release.wait(5):
                    raise AssertionError('Barrier timed out')
            return put(params)

        with patch.object(self.broker, 'put_message', side_effect=hold):
            connection = socket.create_connection(('127.0.0.1', self.server.server_port), timeout=3)
            try:
                body = json.dumps({'method': 'put_message', 'params': {'stream': 'disconnect', 'value': 'disconnected'}}).encode()
                connection.sendall(b'POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: '
                                   + str(len(body)).encode() + b'\r\n\r\n' + body)
                self.assertTrue(entered.wait(2))
                connection.close()
                following = self.pool.submit(self.request, 'put_message', stream='disconnect', value='following')
                self.wait_for_pending('disconnect', 2)
            finally:
                connection.close()
                self.release.set()
            self.assertEqual(following.result(timeout=3)[1]['index'], 1)
        self.wait_for_pending('disconnect', 0)

    def test_shutdown_cancels_waiting_stream_requests_and_releases_threads(self):
        entered = Event()
        put = self.broker.put_message

        def hold(params):
            entered.set()
            if not self.release.wait(5):
                raise AssertionError('Barrier timed out')
            return put(params)

        dispatcher = self.server.task_dispatcher
        with patch.object(self.broker, 'put_message', side_effect=hold):
            first = self.pool.submit(self.request, 'put_message', stream='shutdown', value=0)
            self.assertTrue(entered.wait(2))
            waiting = self.pool.submit(self.request, 'put_message', stream='shutdown', value=1)
            try:
                self.wait_for_pending('shutdown', 2)
                dispatcher.shutdown(timeout=.1)
                self.assertFalse(dispatcher.admission_thread.is_alive())
                self.assertFalse(dispatcher.streams)
                with self.assertRaises(requests.RequestException):
                    waiting.result(timeout=2)
            finally:
                self.release.set()
            self.assertEqual(first.result(timeout=3)[1]['index'], 0)
        dispatcher.shutdown(timeout=2)
        self.assertFalse(dispatcher.threads)
        self.assertFalse(dispatcher.rejections.threads)
        self.assertEqual((dispatcher.pending_requests, dispatcher.pending_bytes), (0, 0))
        self.assertFalse(dispatcher.stream_bytes)
        self.assertEqual(self.broker._get_partition('shutdown', 0)._get_index(), 0)
