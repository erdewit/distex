import os
import sys
import asyncio
import logging
import traceback
import concurrent.futures
import collections.abc
from enum import IntEnum
from collections import deque
from contextlib import suppress

from .worker import Worker
from .slotpool import SlotPool
from .serializer import ClientSerializer, PickleType
from . import util


class LoopType(IntEnum):
    default = 0
    asyncio = 1
    uvloop = 2
    proactor = 3
    quamash = 4


class Pool:
    """
    Pool of local and remote workers that can run tasks.

    To create a process pool of 4 local workers:

    .. code-block:: python

        pool = Pool(4)

    To create 8 remote workers on host ``maxi``, using SSH (unix only):

    .. code-block:: python

        pool = Pool(0, 'ssh://maxi/8')

    ``distex`` must be installed on all remote hosts and the ``distex_proc``
    script must be in the path. Test this with ``ssh <host> distex_proc``.
    When using SSH it is not necessary to have a distex server running
    on the hosts.

    When not using SSH a spawning server has to be started
    first on all hosts involved:

    .. code-block:: python

        python3 -m distex.server

    .. warning::

        Only use this in a trusted network environment.

    With the server running on host ``mini``,
    to create a pool of 2 workers running there:

    .. code-block:: python

        pool = Pool(0, 'mini/2')

    Local, remote SSH and remote non-SSH workers can all be
    combined in one pool:

    .. code-block:: python

        pool = Pool(4, ['ssh://maxi/8', 'mini/2'])

    To give a SSH username or a non-default port such as 10022, specify the
    host as ``'ssh://username@maxi:10022/8'``.
    It is not possible to give a password,
    use SSH keys instead: ssh-keygen_ can be used to create a key and
    ssh-copy-id_ to copy it to all hosts.
    """

    TimeoutError = concurrent.futures.TimeoutError

    def __init__(
            self,
            num_workers: int = None,
            hosts=None,
            qsize: int = 2,
            initializer=None,
            initargs: tuple = (),
            localhost: str = None,
            localport: int = None,
            lazy_create: bool = False,
            worker_loop=LoopType.default,
            func_pickle=PickleType.dill,
            data_pickle=PickleType.pickle):
        """
        Args:
            num_workers: Number of local process workers. The default of
              None will use the number of CPUs.
            hosts: List of remote host specification strings in the format
              ``[ssh://][username@]hostname[:portnumber]/num_workers``.

            qsize: Number of pending tasks per worker.
              To improve the throughput of small tasks this can be increased
              from the default of 2.
              If no queueing is desired then it can be set to 1.
            initializer: Callable to initialize worker processes.
            initargs: Arguments tuple that is unpacked into the initializer.
            localhost: Local TCP server (if any) will listen on this address.
            localport: Local TCP server (if any) will listen on this port
              (default: random open port).
            lazy_create: If True then no workers will be created until the
              first task is submitted.
            worker_loop:
              ``LoopType`` to use for workers:

                0. default (=uvloop when available, proactor on Windows)
                1. asyncio (standard selector event loop)
                2. uvloop (Unix only)
                3. proactor (Windows only)
                4. quamash (PyQt)
            func_pickle: ``PickleType`` to to use for serializing functions:

                0. pickle
                1. cloudpickle
                2. dill
            data_pickle: ``PickleType`` to to use for data:

                0. pickle
                1. cloudpickle
                2. dill

        ``distex.Pool`` implements the ``concurrent.futures.Executor``
        interface and can be used in the place of ProcessPoolExecutor.

        .. _ssh-keygen: https://linux.die.net/man/1/ssh-keygen
        .. _ssh-copy-id: https://linux.die.net/man/1/ssh-copy-id
        """
        self._num_workers = num_workers if num_workers is not None \
            else os.cpu_count()
        self._hosts = [hosts] if type(hosts) is str else hosts if hosts else []
        self._qsize = qsize
        self._initializer = initializer
        self._initargs = initargs
        self._localhost = localhost
        self._localport = localport
        self._loop = asyncio.get_event_loop()
        self._worker_loop = int(worker_loop)
        self._func_pickle = int(func_pickle)
        self._data_pickle = int(data_pickle)
        if self._num_workers < 0:
            raise ValueError('num_workers must be >= 0')
        if self._qsize < 1:
            raise ValueError('qsize must be >= 1')
        self.ready = asyncio.Event()
        self._worker_added = asyncio.Event()
        self._logger = logging.getLogger('distex.Pool')
        self._reset()
        if not lazy_create and not self._loop.is_running():
            self._loop.run_until_complete(self.create())

    def _reset(self):
        self._tcp_server = None
        self._unix_server = None
        self._unix_path = ''
        self._ssh_tunnels = []
        self._procs = []
        self._total_workers = 0
        self._workers = []
        self._slots = SlotPool()
        self._create_called = False

    def __enter__(self):
        return self

    def __exit__(self, *_excinfo):
        self.shutdown()

    async def __aenter__(self):
        await self.create()
        return self

    async def __aexit__(self, *_excinfo):
        await self.shutdown_async()

    def __await__(self):
        return self.create().__await__()

    async def create(self):
        """
        Coroutine to create local processors and servers and
        start up remote processors.
        """
        if self._create_called:
            return
        self._create_called = True

        hostSpecs = [HostSpec(host) for host in self._hosts]

        args = [
            '-f', self._func_pickle,
            '-d', self._data_pickle]
        if sys.platform == "win32":
            await self._start_tcp_server()
            args += [
                '-H', self._localhost or '127.0.0.1',
                '-p', self._localport,
                '-l', self._worker_loop]
        else:
            if not all(spec.is_ssh for spec in hostSpecs):
                await self._start_tcp_server()
            await self._start_unix_server()
            args += [
                '-u', self._unix_path,
                '-l', self._worker_loop]
        await self._start_local_processors(args)

        tasks = [self._add_host(spec) for spec in hostSpecs]
        await asyncio.gather(*tasks)

        while len(self._workers) < self._total_workers:
            await self._worker_added.wait()

        await asyncio.sleep(0)  # needed for lazy_create
        if self._initializer:
            tasks = [
                worker.run_task(
                    (self._initializer, self._initargs, {}, True, False))
                for worker in self._workers]
            await asyncio.gather(*tasks)
        self.ready.set()
        return self

    async def _start_unix_server(self):
        # start server that listens on a Unix socket
        self._unix_path = util.get_temp_path()
        self._unix_server = await self._loop.create_unix_server(
            self._create_worker,
            self._unix_path)
        self._logger.info('Started serving on Unix socket %s', self._unix_path)

    async def _start_tcp_server(self):
        # start server that listens on a TCP port
        localhost = self._localhost or (
            '0.0.0.0' if self._hosts else '127.0.0.1')
        if not self._localport:
            self._localport = util.get_random_port()
        self._tcp_server = await self._loop.create_server(
            self._create_worker,
            localhost, self._localport)
        self._logger.info(f'Started serving on port {self._localport}')

    async def _add_host(self, spec):
        if spec.is_ssh:
            await self._start_remote_processors_ssh(
                spec.host, spec.port, spec.num_workers)
        else:
            await self._start_remote_processors(
                spec.host, spec.port, spec.num_workers)

    async def _start_local_processors(self, args):
        # spawn processors that will connect to our Unix or TCP server
        tasks = [
            self._loop.subprocess_exec(
                asyncio.SubprocessProtocol,
                'distex_proc', *(str(arg) for arg in args),
                stdout=None, stderr=None)
            for _ in range(self._num_workers)]
        self._procs = await asyncio.gather(*tasks)
        self._total_workers += self._num_workers

    async def _start_remote_processors(self, host, port, num_workers):
        # connect to remote server and tell how much processors to spawn and
        # on what port they can find our TCP server
        _reader, writer = await asyncio.open_connection(host, port)
        writer.write(b'%d %d %d %d %d\n' % (
            num_workers,
            self._localport,
            self._worker_loop,
            self._func_pickle,
            self._data_pickle))
        await writer.drain()
        writer.close()
        self._total_workers += num_workers

    async def _start_remote_processors_ssh(self, host, port, num_workers):
        # establish a reverse SSH tunnel from remote unix socket to
        # the local unix socket that our Unix server is listening on
        port_arg = ('-p', port) if port else ()
        remote_unix_path = util.get_temp_path()
        proc = await asyncio.create_subprocess_exec(
            'ssh',
            '-T', host,
            *port_arg,
            '-R', f'{remote_unix_path}:{self._unix_path}',
            stdin=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        # spawn processors that will connect to the tunneled Unix server
        cmd = (
            f'distex_proc '
            f'-u {remote_unix_path} '
            f'-l {self._worker_loop} '
            f'-f {self._func_pickle} '
            f'-d {self._data_pickle} '
            f'& \n'.encode()) * num_workers
        proc.stdin.write(cmd)
        await proc.stdin.drain()
        self._ssh_tunnels.append(proc)
        self._total_workers += num_workers

    def _create_worker(self):
        serializer = ClientSerializer(self._func_pickle, self._data_pickle)
        worker = Worker(serializer)
        worker.disconnected = self._on_worker_disconnected
        self._workers.append(worker)
        self._slots.extend((worker,) * self._qsize)
        self._worker_added.set()
        self._worker_added.clear()
        return worker

    def _on_worker_disconnected(self, worker):
        pass

    def is_ready(self) -> bool:
        """
        True if the pool is ready to process tasks, false otherwise.

        There is also the public ``ready`` event.
        """
        return self.ready.is_set()

    def total_workers(self) -> int:
        """
        Total number of workers in the pool.
        """
        return self._total_workers

    def submit(self, func, *args, **kwargs):
        """
        Submit the task to be run in the pool and return a
        concurrent.futures.Future that will hold the result.

        This method is provided for compatibility with
        concurrent.futures.Executor.
        """
        future = concurrent.futures.Future()
        task = self._loop.create_task(self.run_async(func, *args, **kwargs))

        def on_task_done(task):
            if task.exception():
                future.set_exception(task.exception())
            else:
                future.set_result(task.result())

        task.add_done_callback(on_task_done)
        return future

    def map(self, func, *iterables,
            timeout=None, chunksize=1, ordered=True, star=False):
        """
        Map the function onto the given iterable(s) and
        return an iterator that yields the results.

        Args:

            func: Function to map. If it returns an awaitable then the
              result is awaited and returned.
            iterables: Sync or async iterables (in any combination)
              that yield the arguments for ``func``. The iterables can
              be unbounded (i.e. they don't need to have a length).
            timeout: Timeout in seconds since map is started.
            chunksize: Iterator is chunked up to this size.
              A larger chunksize can greatly improve efficiency
              for small tasks.
            ordered:
                * ``True``: The order of results preserves the
                  order of the input iterables.
                * ``False``: The results are in order of completion.
            star:
                * ``True``: There can be only one iterable and it must
                  yield sequences (such as tuples). The sequences are
                  unpacked ('starred') into ``func``.
                * ``False``: The values that the iterators yield are
                  supplied in-place to ``func``.

        .. tip::

           The function ``func`` is is pickled only once and then cached.
           If it takes arguments that remain constant during the mapping then
           consider using ``functools.partial`` to bind the function with the
           constant arguments; Then do the mapping with the bound function and
           with lesser arguments. Especially when map uses large constant
           datasets this can be beneficial.
        """
        run = self._loop.run_until_complete
        agen = self._map(func, iterables, timeout, chunksize, ordered, star)
        nxt = agen.__anext__
        try:
            if chunksize == 1:
                while True:
                    yield run(nxt())
            else:
                while True:
                    yield from run(nxt())
        except StopAsyncIteration:
            pass

    async def map_async(
            self, func, *iterables, timeout=None,
            chunksize=1, ordered=True, star=False):
        """
        Async version of ``map``. This runs with less overhead than ``map``
        and can be twice as fast for small tasks.
        """
        agen = self._map(func, iterables, timeout, chunksize, ordered, star)
        if chunksize == 1:
            async for result in agen:
                yield result
        else:
            async for results in agen:
                for r in results:
                    yield r

    async def _map(self, func, iterables, timeout, chunksize, ordered, star):
        if not self._create_called:
            await self.create()
        await self.ready.wait()

        end_time = None if timeout is None else self._loop.time() + timeout
        tasks = deque()
        create_task = self._loop.create_task
        run_task = self._run_task
        input_consumed = False
        do_map = chunksize > 1
        is_sync = all(
            isinstance(it, collections.abc.Iterable) for it in iterables)

        if is_sync:
            if len(iterables) > 1:
                it = zip(*iterables)
                star = True
            else:
                it = iterables[0].__iter__()
            if do_map:
                get_args = util.chunk(it, chunksize).__next__
            else:
                get_args = it.__next__
        else:
            if len(iterables) > 1:
                it = util.zip_async(*iterables)
                star = True
            else:
                it = iterables[0].__aiter__()
            if do_map:
                get_args = util.chunk_async(it, chunksize).__anext__
            else:
                get_args = it.__anext__

        try:
            while True:
                try:
                    # schedule as many tasks as possible
                    for _ in range(self._slots.num_free):
                        args = get_args() if is_sync else await get_args()
                        tasks.append(
                            create_task(
                                run_task((func, args, None, star, do_map))))
                except (StopIteration, StopAsyncIteration):
                    input_consumed = True

                if not tasks and input_consumed:
                    # we're finished
                    break

                # wait for a slot to become ready
                ready = self._slots.slot_ready()
                if timeout is not None:
                    ready = asyncio.wait_for(
                        ready, end_time - self._loop.time())
                await ready

                # yield as many results as possible
                if ordered:
                    while tasks and tasks[0].done():
                        yield tasks.popleft().result()
                else:
                    for task in tasks:
                        if task.done():
                            yield task.result()
                    tasks = deque(task for task in tasks if not task.done())

        except asyncio.TimeoutError:
            raise self.TimeoutError()

    async def _run_task(self, task):
        worker = await self._slots.get()
        try:
            success, result = await worker.run_task(task)
        finally:
            self._slots.put(worker, not(worker.tasks))
        if success:
            return result
        raise result

    def run(self, func, *args, **kwargs):
        """
        Run the function with the given arguments
        in the pool and wait for the result.
        """
        return self._loop.run_until_complete(
            self.run_async(func, *args, **kwargs))

    async def run_async(self, func, *args, **kwargs):
        """
        Asynchronously run the function with the given arguments
        in the pool and return the result when it becomes available.
        """
        if not self._create_called:
            await self.create()
        await self.ready.wait()

        return await self._run_task((func, args, kwargs, True, False))

    def run_on_all(self, func, *args, **kwargs):
        """
        Run the task on each worker in the pool. Return a list of all results
        (in order of completion) or raise an exception in case the task fails
        on one or more workers.

        Will first wait for any other pending tasks to finish and then
        schedule the task over all workers at the same time.

        This can be used for initializing, cleanup, intermittent polling, etc.
        """
        return self._loop.run_until_complete(
            self.run_on_all_async(func, *args, **kwargs))

    async def run_on_all_async(self, func, *args, **kwargs):
        """
        Async version of ``run_on_all``.
        """
        if not self._create_called:
            await self.create()
        await self.ready.wait()

        await self._drain()

        tasks = [
            worker.run_task((func, args, kwargs, True, False))
            for worker in self._workers]
        results = await asyncio.gather(*tasks)
        for success, result in results:
            if not success:
                raise result
        return [result for _, result in results]

    async def _drain(self):
        """
        Let all current tasks finish.
        """
        tasks = [self._slots.get() for _ in range(self._slots.capacity)]
        slots = await asyncio.gather(*tasks)
        for slot in slots:
            self._slots.put(slot)

    def shutdown(self, wait=True):
        """
        Shutdown the pool and clean up resources.
        """
        coro = self.shutdown_async(wait)
        if self._loop.is_running():
            asyncio.ensure_future(coro)
        else:
            self._loop.run_until_complete(coro)

    async def shutdown_async(self, wait=True):
        if not self._total_workers:
            return
        if wait:
            await self._drain()
        for worker in self._workers:
            worker.stop()
        for transport, protocol in self._procs:
            transport.close()
        if self._unix_server:
            self._unix_server.close()
            with suppress(FileNotFoundError):
                os.unlink(self._unix_path)
        if self._tcp_server:
            self._tcp_server.close()
        self._reset()


class RemoteException(Exception):
    """
    Proxy for an exception that occurs remotely while running a task.
    """

    def __init__(self, exc, tb=None):
        self.exc = exc
        tb = tb or traceback.format_exception(
            type(exc), exc, exc.__traceback__)
        self.tb = ''.join(tb)

    def __str__(self):
        return self.tb

    def __reduce__(self):
        return RemoteException._unpickle, (self.exc, self.tb)

    @staticmethod
    def _unpickle(exc, tb):
        exc.__cause__ = RemoteException(None, tb)
        return exc


class HostSpec:
    """
    Remote host specification.
    """
    __slots__ = ('num_workers', 'is_ssh', 'host', 'port')

    def __init__(self, url):
        """
        Parse the host url string.
        """
        front, ssh, back, = url.partition('ssh://')
        h_p, *nw = (back if ssh else front).split('/')
        if not h_p:
            raise ValueError(f'Bad server specification: {url}')
        if not nw:
            raise ValueError(f'Specify num_workers for {url}')
        self.num_workers = int(nw[0])
        self.host, *port = h_p.split(':')
        self.port = port[0] if port else '' if ssh else str(util.DEFAULT_PORT)
        self.is_ssh = bool(ssh)
