import asyncio
from collections import deque


class SlotPool:
    """
    Pool that manages a limited number of contendable resource slots.
    """
    __slots__ = ('num_free', 'capacity', '_slots',
            '_loop', '_get_waiters', '_slot_ready_waiter')

    def __init__(self, *, loop=None):
        self.num_free = 0
        self.capacity = 0
        self._slots = deque()
        self._loop = loop or asyncio.get_event_loop()
        self._get_waiters = deque()
        self._slot_ready_waiter = None

    def extend(self, slots):
        """
        Add new slots to the pool.
        """
        if not slots:
            return
        slots_length = len(slots)
        self.capacity += slots_length
        self.num_free += slots_length
        self._slots.appendleft(slots[0])
        self._slots.extend(slots[1:])
        self._wake_up_next()

    def _wake_up_next(self):
        while self._get_waiters:
            waiter = self._get_waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                return

    async def get(self):
        """
        Await a free slot from the pool.
        """
        while not self._slots:
            fut = self._loop.create_future()
            self._get_waiters.append(fut)
            try:
                await fut
            except:
                fut.cancel()
                if self._slots and not fut.cancelled():
                    self._wake_up_next()
                raise
        self.num_free -= 1
        return self._slots.popleft()

    def put(self, slot, front=True):
        """
        Put the slot back in the pool again.
        """
        self.num_free += 1
        if front:
            self._slots.appendleft(slot)
        else:
            self._slots.append(slot)
        self._wake_up_next()
        if self._slot_ready_waiter:
            self._slot_ready_waiter.set_result(None)
            self._slot_ready_waiter = None

    def slot_ready(self):
        """
        Awaitable signal when a slot has been put back with ``put``.
        """
        if self._slot_ready_waiter is None:
            self._slot_ready_waiter = self._loop.create_future()
        return self._slot_ready_waiter
