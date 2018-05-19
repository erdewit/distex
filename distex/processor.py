import asyncio
import argparse
import logging
import signal

from distex.pool import LoopType, RemoteException
from distex.serializer import Serializer
from distex import util

signal.signal(signal.SIGINT, signal.SIG_IGN)



class Processor(asyncio.Protocol):
    """
    Single process that works on tasks.
    """

    def __init__(self, host, port, unix_path, loop):
        self._host = host
        self._port = port
        self._unix_path = unix_path
        self._loop = loop
        self._queue = asyncio.Queue()
        self._transport = None
        self._last_func = None
        self._serializer = Serializer()
        self._worker_task = loop.create_task(self.worker())
        self._logger = logging.getLogger('distex.Processor')
        loop.run_until_complete(self.create())

    async def create(self):
        if self._unix_path:
            self._transport, _ = await self._loop.create_unix_connection(
                    lambda: self, self._unix_path)
        else:
            self._transport, _ = await self._loop.create_connection(
                    lambda: self, self._host, self._port)

    async def worker(self):
        while True:
            func, args, kwargs, do_star, do_map = await self._queue.get()
            try:
                if do_map:
                    if do_star:
                        result = [func(*a) for a in args]
                    else:
                        result = [func(a) for a in args]
                    if result and hasattr(result[0], '__await__'):
                        result = [await r for r in result]
                else:
                    if do_star:
                        result = func(*args, **kwargs)
                    else:
                        result = func(args)
                    if hasattr(result, '__await__'):
                        result = await result
                success = 1
            except Exception as e:
                result = RemoteException(e)
                success = 0
            del func, args, kwargs
            self._serializer.write_response(
                    self._transport.write, success, result)
            del result

    def peername(self):
        if self._unix_path:
            return f'{self._unix_path}'
        else:
            return f'{self._host}:{self._port}'

    def connection_made(self, _transport):
        self._logger.info(f'Connected to {self.peername()}')

    def connection_lost(self, exc):
        self._loop.stop()
        if exc:
            self._logger.error(f'Connection lost from {self.peername()}: {exc}')

    def data_received(self, data):
        for task in self._serializer.get_requests(data):
            self._queue.put_nowait(task)


def main():
    parser = argparse.ArgumentParser(
            description='Run a single task processor',
            formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--host', '-H', dest='host',
            type=str, help='connect to host')
    parser.add_argument('--port', '-p', dest='port',
            type=int, help='port number')
    parser.add_argument('--unix_path', '-u', dest='unix_path',
            type=str, help='connect to Unix domain socket')
    parser.add_argument('--loop', '-l', dest='loop', default=0,
            type=int, help='0=default 1=asyncio 2=uvloop 3=proactor 4=quamash')
    args = parser.parse_args()

    if args.loop == LoopType.default:
        loop = util.get_loop()
    elif args.loop == LoopType.asyncio:
        loop = asyncio.get_event_loop()
    elif args.loop == LoopType.uvloop:
        import uvloop
        loop = uvloop.Loop()
    elif args.loop == LoopType.proactor:
        loop = asyncio.ProactorEventLoop()
    elif args.loop == LoopType.quamash:
        import quamash
        import PyQt5.Qt as qt
        _qapp = qt.QApplication([])
        loop = quamash.QEventLoop()
    asyncio.set_event_loop(loop)
    processor = Processor(args.host, args.port, args.unix_path, loop=loop)
    loop.run_forever()


if __name__ == '__main__':
    main()
