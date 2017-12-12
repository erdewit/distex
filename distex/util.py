import sys
import asyncio
import socket
import logging
import uuid
import itertools
from contextlib import suppress

DEFAULT_PORT = 8899


async def zip_async(*sync_or_async_iterables):
    """
    Asynchronous zip: Create iterator that yields the aggregated elements
    of the given synchronous or asynchronous iterables as tuples.
    """
    nxts = [next_method(it) for it in sync_or_async_iterables]
    try:
        while True:
            r = [nxt() if is_sync else await nxt() for nxt, is_sync in nxts]
            yield tuple(r)
    except (StopIteration, StopAsyncIteration):
        pass


def chunk(it, chunksize):
    """
    Create iterator that chunks together the yielded values of the
    given iterable into tuples of ``chunksize`` long.
    The last tuple can be shorter if ``it`` is exhausted.
    """
    while True:
        t = tuple(itertools.islice(it, chunksize))
        if not t:
            break
        yield t


async def chunk_async(ait, chunksize):
    """
    Same as chunk but for asynchronous iterable.
    """
    nxt = ait.__aiter__().__anext__
    r = []
    try:
        while True:
            for _ in range(chunksize):
                r.append(await nxt())
            yield tuple(r)
            del r[:]
    except (StopIteration, StopAsyncIteration):
        pass
    if r:
        yield tuple(r)


def next_method(it):
    """
    Get the method that yields next value from the given sync or async iterable.
    Returns (method, is_sync) tuple of the method and a boolean of whether
    the iterable is synchronous or not.
    """
    if hasattr(it, '__next__'):
        nxt = it.__next__
        is_sync = True
    elif hasattr(it, '__iter__'):
        nxt = it.__iter__().__next__
        is_sync = True
    elif hasattr(it, '__anext__'):
        nxt = it.__anext__
        is_sync = False
    elif hasattr(it, '__aiter__'):
        nxt = it.__aiter__().__anext__
        is_sync = False
    else:
        raise ValueError(f'{it} is not iterable')
    return nxt, is_sync


def get_loop():
    """
    Get optimal event loop for the platform.
    """
    loop = None
    if sys.platform == 'win32':
        loop = asyncio.ProactorEventLoop()
    else:
        with suppress(ImportError):
            import uvloop
            loop = uvloop.Loop()
    return loop or asyncio.get_event_loop()


def single_step(loop):
    """
    Do a single step of the asyncio event loop.
    """
    loop.call_soon(loop.stop)
    loop.run_forever()


def get_random_port():
    """
    Get a random open port number on localhost.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


def get_temp_path():
    """
    Get a universally unique temporary filename.
    """
    return f'/tmp/distex-{uuid.uuid1().hex}'


def logToFile(path, level=logging.INFO):
    """
    Create a log handler that logs to the given file.
    """
    logger = logging.getLogger()
    logger.setLevel(level)
    formatter = logging.Formatter(
            '%(asctime)s %(name)s %(levelname)s %(message)s')
    handler = logging.FileHandler(path)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def logToConsole(level=logging.INFO):
    """
    Create a log handler that logs to the console.
    """
    logger = logging.getLogger()
    logger.setLevel(level)
    formatter = logging.Formatter(
            '%(asctime)s %(name)s %(levelname)s %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
