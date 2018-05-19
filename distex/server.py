import os
import sys
import asyncio
import logging
import argparse

from . import util



class Server:
    """
    Serve requests from remote pools to spawn local processors.
    Each spawned processor will by itself connect back to the requesting pool.
    
    Use only in a trusted network environment.
    """

    def __init__(self, host='0.0.0.0', port=util.DEFAULT_PORT, loop=None):
        self._host = host
        self._port = port
        self._loop = loop or asyncio.get_event_loop()
        self._server = None
        self._logger = logging.getLogger('distex.Server')
        self._loop.run_until_complete(self.create())

    async def create(self):
        self._server = await asyncio.start_server(
                self.handle_request, self._host, self._port, loop=self._loop)
        self._logger.info(f'Serving on port {self._port}')

    async def handle_request(self, reader, writer):
        req_host, req_port = writer.get_extra_info('peername')
        peername = f'{req_host}:{req_port}'
        _logger.info(f'Connection from {peername}')
        data = await reader.readline()
        nw, port, worker_loop = data.split()
        num_workers = int(nw) or os.cpu_count()
        self._logger.info(f'Starting up {num_workers} processors for {peername}')

        # start processors that will connect back to the remote server
        asyncio.gather(*[asyncio.create_subprocess_exec(
                'distex_proc',
                '-H', req_host, '-p', port, '-l', worker_loop,
                stdout=None, stderr=None, loop=self._loop)
                for _ in range(num_workers)])

        writer.close()

    def stop(self):
        self._server.close()
        self._logger.info(f'Stopped serving from {self._port}')

    def run(self):
        try:
            self._loop.run_forever()
        except KeyboardInterrupt:
            self._server.close()
            self._loop.run_until_complete(self._server.wait_closed())
            self._loop.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description=('Run a process-spawning distex server. '
                    'Use only in a trusted network environment.'),
            formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--host', '-H', dest='host', default='0.0.0.0',
            type=str, help='local host to serve from')
    parser.add_argument('--port', '-p', dest='port', default=util.DEFAULT_PORT,
            type=int, help='port number to serve from')
    args = parser.parse_args()

    util.logToConsole()

    server = Server(args.host, args.port)
    server.run()
