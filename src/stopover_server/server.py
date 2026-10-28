from collections import deque
from itertools import chain
from sys import getsizeof
from threading import Condition, Thread

import falcon
from waitress import create_server as waitress_server
from waitress.task import ThreadedTaskDispatcher, WSGITask
from waitress.utilities import Error, RequestEntityTooLarge

from . import utils


class QueueFull(Error):
    code = 503
    reason = 'Service Unavailable'

    def to_response(self, ident=None):
        status, headers, body = super().to_response(ident)
        return status, [*headers, ('Retry-After', '1')], body


def payload_size(value, limit):
    """Conservative object-size budget; shared scalar references count again."""
    size = 0
    pending = [iter((value,))]
    while pending:
        try:
            item = next(pending[-1])
        except StopIteration:
            pending.pop()
            continue
        size += getsizeof(item)
        if size > limit:
            return size
        if isinstance(item, dict):
            pending.append(chain(item.keys(), item.values()))
        elif isinstance(item, (list, tuple)):
            pending.append(iter(item))
    return size


class PreparedTask(WSGITask):
    def get_environment(self):
        environ = super().get_environment()
        if hasattr(self.request, 'stopover_request'):
            environ['stopover.request'] = self.request.stopover_request
        return environ


class StreamTask:
    def __init__(self, dispatcher, channel, size):
        self.dispatcher = dispatcher
        self.channel = channel
        self.request = channel.requests[0]
        self.stream = None
        self.size = size
        self.accounted = True

    def service(self):
        try:
            self.channel.service()
        finally:
            self.dispatcher.completed(self)

    def cancel(self):
        self.channel.cancel()
        self.request.close()
        self.dispatcher.completed(self)


class StreamDispatcher(ThreadedTaskDispatcher):
    STREAM_METHODS = {'put_message', 'get_message', 'get_partitions',
                      'commit_message', 'set_offset'}
    DEFAULT_LIMITS = {
        'max_pending_requests': 256,
        'max_pending_bytes': 512 * 1024 * 1024,
        'max_stream_pending_requests': 64,
        'max_stream_pending_bytes': 256 * 1024 * 1024,
        'max_request_bytes': 128 * 1024 * 1024,
    }

    def __init__(self, broker, threads, limits):
        super().__init__()
        self.broker = broker
        self.incoming = deque()
        self.streams = {}
        self.stream_bytes = {}
        self.limits = limits
        self.pending_requests = 0
        self.pending_bytes = 0
        self.closing = False
        self.admission_cv = Condition(self.lock)
        self.rejections = ThreadedTaskDispatcher()
        self.rejections.set_thread_count(1)
        self.set_thread_count(threads)
        self.admission_thread = Thread(target=self.admit, name='stopover-admission', daemon=True)
        self.admission_thread.start()

    def add_task(self, channel):
        request = channel.requests[0]
        if request.error:
            self.reject(channel, request.error)
            return
        body = request.get_body_stream()
        position = body.tell()
        body.seek(0, 2)
        size = body.tell()
        body.seek(position)
        if size > min(self.limits['max_request_bytes'], self.limits['max_pending_bytes']):
            self.reject(channel, RequestEntityTooLarge('Request body exceeds the size limit'))
            return
        with self.lock:
            if (not self.closing and self.pending_requests < self.limits['max_pending_requests']
                    and self.pending_bytes + size <= self.limits['max_pending_bytes']):
                self.pending_requests += 1
                self.pending_bytes += size
                self.incoming.append(StreamTask(self, channel, size))
                self.admission_cv.notify()
                return
        self.reject(channel, QueueFull('Broker pending request limit reached'))

    def reject(self, channel, error):
        # Use a separate, bounded responder so a saturated worker pool can reject.
        for request in channel.requests:
            request.close()
            request.body_rcv = None
            if hasattr(request, 'stopover_request'):
                del request.stopover_request
        if not channel.requests:
            channel.cancel()
            return
        channel.requests[0].error = error
        with self.rejections.lock:
            if not self.closing and len(self.rejections.queue) < self.limits['max_pending_requests']:
                self.rejections.queue.append(channel)
                self.rejections.queue_cv.notify()
                return
        channel.cancel()

    def prepare(self, task):
        channel, request = task.channel, task.request
        channel.task_class = PreparedTask
        if request.error or request.command.upper() != 'POST':
            return None
        headers = {key.lower(): value for key, value in request.headers.items()}
        if 'auth' in self.broker.config and not self.broker.check_authorized(headers):
            return None
        body = request.get_body_stream()
        position = body.tell()
        try:
            data, plain = utils.decode_request(body.read(), min(
                self.limits['max_request_bytes'], self.limits['max_pending_bytes']))
        finally:
            body.seek(position)
        request.stopover_request = data, plain
        if (not isinstance(data, dict) or not isinstance(data.get('method'), str)
                or data['method'] not in self.STREAM_METHODS):
            return None
        params = data.get('params')
        if not isinstance(params, dict):
            return None
        stream = params.get('stream')
        try:
            self.broker._get_stream_path(stream)
        except ValueError:
            return None
        return stream

    def enqueue(self, task):
        self.queue.append(task)
        self.queue_cv.notify()

    def admit(self):
        while True:
            with self.lock:
                while not self.incoming and not self.closing:
                    self.admission_cv.wait()
                if self.closing:
                    return
                task = self.incoming.popleft()
            channel = task.channel
            if not channel.connected or not channel.requests:
                task.cancel()
                continue
            try:
                stream = self.prepare(task)
            except utils.RequestTooLarge as error:
                self.completed(task)
                with channel.requests_lock:
                    self.reject(channel, RequestEntityTooLarge(str(error)))
                continue
            except Exception:
                self.logger.exception('Could not prepare broker request')
                task.cancel()
                continue
            data = getattr(task.request, 'stopover_request', (None, False))[0]
            size = task.size + payload_size(data, self.limits['max_pending_bytes'])
            del data
            maximum = min(self.limits['max_pending_bytes'], self.limits['max_stream_pending_bytes']) if stream else self.limits['max_pending_bytes']
            error = RequestEntityTooLarge('Request cannot fit the byte budget') if size > maximum else QueueFull('Broker pending request limit reached')
            with self.lock:
                if (not self.closing and size <= maximum
                        and self.pending_bytes + size - task.size <= self.limits['max_pending_bytes']
                        and (stream is None or (
                            len(self.streams.get(stream, ())) < self.limits['max_stream_pending_requests']
                            and self.stream_bytes.get(stream, 0) + size <= self.limits['max_stream_pending_bytes']))):
                    self.pending_bytes += size - task.size
                    task.size = size
                    if stream is None:
                        self.enqueue(task)
                    else:
                        task.stream = stream
                        self.stream_bytes[stream] = self.stream_bytes.get(stream, 0) + size
                        pending = self.streams.setdefault(stream, deque())
                        pending.append(task)
                        if len(pending) == 1:
                            self.enqueue(task)
                    continue
            self.completed(task)
            with channel.requests_lock:
                self.reject(channel, error)

    def completed(self, task):
        with self.lock:
            if not task.accounted:
                return
            task.accounted = False
            self.pending_requests -= 1
            self.pending_bytes -= task.size
            if hasattr(task.request, 'stopover_request'):
                del task.request.stopover_request
            if task.stream is None:
                return
            self.stream_bytes[task.stream] -= task.size
            if self.stream_bytes[task.stream] == 0:
                del self.stream_bytes[task.stream]
            pending = self.streams.get(task.stream)
            if pending is None:
                return
            pending.popleft()
            if pending:
                self.enqueue(pending[0])
            else:
                del self.streams[task.stream]

    def shutdown(self, cancel_pending=True, timeout=5):
        with self.lock:
            self.closing = True
            cancelled = list(self.incoming)
            self.incoming.clear()
            for pending in self.streams.values():
                cancelled.extend(list(pending)[1:])
            self.streams.clear()
            self.admission_cv.notify()
        for task in cancelled:
            task.cancel()
        self.admission_thread.join(timeout)
        self.rejections.shutdown(cancel_pending=cancel_pending, timeout=timeout)
        return super().shutdown(cancel_pending=cancel_pending, timeout=timeout)


def create_server(broker, threads=4, **options):
    if type(threads) is not int or threads < 1:
        raise ValueError('global.threads must be a positive integer')
    limits = {name: broker.config['global'].get(name, default)
              for name, default in StreamDispatcher.DEFAULT_LIMITS.items()}
    for name, value in limits.items():
        if type(value) is not int or value < 1:
            raise ValueError(f'global.{name} must be a positive integer')
    options.setdefault('max_request_body_size', limits['max_request_bytes'])
    api = falcon.App(cors_enable=True)
    api.add_route('/', broker)
    dispatcher = StreamDispatcher(broker, threads, limits)
    try:
        return waitress_server(api, threads=threads, _dispatcher=dispatcher, **options)
    except BaseException:
        dispatcher.shutdown()
        raise
