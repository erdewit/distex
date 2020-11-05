import asyncio
import logging
from collections import deque

_logger = logging.getLogger('distex.Worker')


class Worker:
    """
    Worker that submits tasks to and gets results from a
    local or remote processor.

    Implements asyncio.Protocol.
    """

    __slots__ = (
        'futures', 'tasks', 'loop', 'disconnected',
        'peername', 'serializer', 'transport')

    def __init__(self, serializer):
        """
        Initialize the connection.

        Args:
            self: (todo): write your description
            serializer: (todo): write your description
        """
        self.serializer = serializer
        self.loop = asyncio.get_event_loop()
        self.transport = None
        self.peername = None
        self.disconnected = None
        self.futures = deque()
        self.tasks = deque()

    def __repr__(self):
        """
        Return a repr representation of a repr__.

        Args:
            self: (todo): write your description
        """
        return f'<Worker {self.peername}>'

    def run_task(self, task):
        """
        Send the task to the processor and return Future for the result.
        """
        future = self.loop.create_future()
        self.futures.append(future)
        self.tasks.append(task)
        self.serializer.write_request(self.transport.write, task)
        return future

    def stop(self):
        """
        Close connection to the processor.
        """
        if self.transport:
            self.transport.close()
            self.transport = None

    # protocol callbacks:

    def connection_made(self, transport):
        """
        Called when a transport.

        Args:
            self: (todo): write your description
            transport: (todo): write your description
        """
        self.transport = transport
        hp = transport.get_extra_info('peername')
        if hp:
            host, port = hp
            self.peername = f'{host}:{port}'
        else:
            self.peername = 'Unix socket'
        _logger.info(f'Connection from {self.peername}')

    def connection_lost(self, exc):
        """
        Called when the connection is closed.

        Args:
            self: (todo): write your description
            exc: (str): write your description
        """
        if exc:
            self.disconnected(self)
            _logger.error(f'Connection lost from {self.peername}: {exc}')
        self.transport = None

    def data_received(self, data):
        """
        Handle incoming data.

        Args:
            self: (todo): write your description
            data: (todo): write your description
        """
        self.serializer.add_data(data)
        for resp in self.serializer.get_responses():
            self.futures.popleft().set_result(resp)
            self.tasks.popleft()

    def eof_received(self):
        """
        Eof_received.

        Args:
            self: (todo): write your description
        """
        pass

    def pause_writing(self):
        """
        Pause the underlying button.

        Args:
            self: (todo): write your description
        """
        pass

    def resume_writing(self):
        """
        Resume a ].

        Args:
            self: (todo): write your description
        """
        pass
