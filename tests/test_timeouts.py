"""Real HTTP/RocksDB acceptance with withheld replies and bounded SDK waits."""

from contextlib import contextmanager
import threading
import time

import requests
from stopover import Stopover, errors
from test_delivery import BrokerFixture


@contextmanager
def withheld_response(server, mode, number=1):
    app = server.get_app()
    release = threading.Event()
    accepted = []

    def delayed(environ, start_response):
        headers = []
        body = app(environ, lambda status, values, *_: headers.append((status, values)))
        try:
            data = b''.join(body)
        finally:
            if hasattr(body, 'close'):
                body.close()
        accepted.append(1)
        hold = len(accepted) == number
        if hold and mode == 'headers':
            release.wait(3)
        start_response(*headers[0])
        if hold and mode == 'body':
            yield data[:1]
            release.wait(3)
            yield data[1:]
        else:
            yield data

    server.set_app(delayed)
    try:
        yield accepted
    finally:
        release.set()
        server.set_app(app)


class TimeoutTests(BrokerFixture):
    def test_all_operations_timeout_without_retrying_or_assuming_rejection(self):
        self.broker.config['global']['partitions'] = 1
        self.publish('original')
        partition = self.broker._get_partition('intake', 0)
        for operation in ('put', 'get', 'commit', 'knock'):
            for mode in ('headers', 'body'):
                with self.subTest(operation=operation, mode=mode):
                    group = operation + '-' + mode
                    client = Stopover(self.endpoint, uid=group, timeout=(.2, .05))
                    self.addCleanup(client.session.close)
                    self.assign(group, group)
                    message = client.get('intake', group)
                    before = partition._get_index()
                    calls = {
                        'put': lambda: client.put('accepted-before-reply', 'intake'),
                        'get': lambda: client.get('intake', group, progress_without_commit=True),
                        'commit': lambda: client.commit(message, group),
                        'knock': lambda: client.knock(group),
                    }
                    start = time.monotonic()
                    with withheld_response(self.server, mode) as accepted:
                        with self.assertRaises((errors.ServerConnectionError, requests.RequestException)):
                            calls[operation]()
                        self.assertEqual(len(accepted), 1)
                    self.assertLess(time.monotonic() - start, 1.5)
                    if operation == 'put':
                        self.assertEqual(partition._get_index(), before + 1)
                    elif operation == 'commit':
                        self.assertEqual(partition._get_offset(group), message.index)
                    elif operation == 'get':
                        self.assertEqual(partition._get_offset(group), -1)
                        self.assertEqual(client.get('intake', group, progress_without_commit=True).index, 0)
                    else:
                        self.assertIn(group, self.broker.last_seen_by_group[group])

    def test_auto_ack_lost_reply_retries_the_same_retained_message(self):
        self.broker.config['global']['partitions'] = 1
        self.publish('first')
        self.publish('second')
        client = Stopover(self.endpoint, uid='auto', timeout=(.2, .05))
        self.addCleanup(client.session.close)
        self.assign('auto', 'auto')
        with withheld_response(self.server, 'body', number=2) as accepted:
            with self.assertRaises(errors.ServerConnectionError):
                client.get('intake', 'auto', auto_commit=True, progress_without_commit=True)
            self.assertEqual(len(accepted), 2)
        self.assertEqual(self.broker._get_partition('intake', 0)._get_offset('auto'), 0)
        self.assertEqual(client.temporary_offsets['intake']['auto']['auto'], {0: 0})
        first = client.get('intake', 'auto', auto_commit=True, progress_without_commit=True)
        second = client.get('intake', 'auto', auto_commit=True, progress_without_commit=True)
        self.assertEqual((first.index, first.value), (0, 'first'))
        self.assertEqual((second.index, second.value), (1, 'second'))
