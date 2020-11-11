"""Proxy for a remote worker process."""

import asyncio
import logging
from collections import deque

_logger = logging.getLogger('distex.Worker')


class Worker(asyncio.Protocol):
    """
    Worker that submits tasks to and gets results from a
    local or remote processor.
    """

    def __init__(self, serializer):
        self.serializer = serializer
        self.loop = asyncio.get_event_loop()
        self.transport = None
        self.peername = ''
        self.disconnected = None
        self.futures = deque()
        self.tasks = deque()

    def __repr__(self):
        return f'<Worker {self.peername}>'

    def run_task(self, task):
        """Send the task to the processor and return Future for the result."""
        self.serializer.write_request(self.transport.write, task)
        future = self.loop.create_future()
        self.futures.append(future)
        self.tasks.append(task)
        return future

    def stop(self):
        """Close connection to the processor."""
        if self.transport:
            self.transport.close()
            self.transport = None

    # protocol callbacks:

    def connection_made(self, transport):
        self.transport = transport
        hp = transport.get_extra_info('peername')
        if hp:
            host, port = hp
            self.peername = f'{host}:{port}'
        else:
            self.peername = 'Unix socket'
        _logger.info(f'Connection from {self.peername}')

    def connection_lost(self, exc):
        if exc:
            self.disconnected(self)
            _logger.error(f'Connection lost from {self.peername}: {exc}')
        self.transport = None

    def data_received(self, data):
        self.serializer.add_data(data)
        for resp in self.serializer.get_responses():
            self.futures.popleft().set_result(resp)
            self.tasks.popleft()

    def eof_received(self):
        pass

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass
