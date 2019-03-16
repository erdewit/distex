import asyncio

from eventkit import Event, Op


class PoolMap(Op):
    """
    Map a function using a distributed pool with
    `eventkit <https://github.com/erdewit/eventkit>`_.

    Args:
        func: Function to map. If it returns an awaitable then the
            result will be awaited and returned.
        timeout: Timeout in seconds since map is started.
        chunksize: Source emits are chunked up to this size.
            A larger chunksize can greatly improve efficiency
            for small tasks.
        ordered:
            * ``True``: The order of results preserves the source order.
            * ``False``: Results are in order of completion.
    """
    __slots__ = ('_pool', '_func', '_task', '_kwargs')

    def __init__(
            self, pool, func, timeout=None, chunksize=1,
            ordered=True, source=None):
        self._pool = pool
        self._func = func
        self._kwargs = dict(
            timeout=timeout,
            chunksize=chunksize,
            ordered=ordered,
            star=True)
        Op.__init__(self, source)

    def set_source(self, source):

        async def looper():
            try:
                async for result in self._pool.map_async(
                        self._func,
                        source.aiter(tuples=True),
                        **self._kwargs):
                    self.emit(result)
            except Exception as error:
                self.error_event.emit(self, error)
            self._task = None
            self.set_done()

        self._task = asyncio.ensure_future(looper())

    def __del__(self):
        if self._task:
            self._task.cancel()


def poolmap(
        self, pool, func,
        timeout=None, chunksize=1, ordered=True):
    return PoolMap(pool, func, timeout, chunksize, ordered, self)


poolmap.__doc__ = PoolMap.__doc__
Event.poolmap = poolmap
