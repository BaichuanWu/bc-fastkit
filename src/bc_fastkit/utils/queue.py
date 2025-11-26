import asyncio
from typing import Generic, TypeVar

T = TypeVar("T")


class QueueClosed(Exception):
    """Raised when attempting to get from a closed and drained queue."""


class AsyncClosableQueue(Generic[T]):
    """An asyncio-compatible queue with producer reference-counted open/close.

    - Producers call `open()` to mark they will produce, and `close()` when done.
      When open-count drops to 0 the queue is closed automatically.
    - The queue can also be closed immediately by `close_force()`.
    - After closed, producers attempting to `put` will raise RuntimeError.
    - Consumers can continue to `get` remaining items. Once the queue is
      closed and drained further `get()` calls raise `QueueClosed`.
    """

    def __init__(self, maxsize: int = 0) -> None:
        self._queue: asyncio.Queue = asyncio.Queue(maxsize)
        self._open_count: int = 0
        self._closed: bool = False
        # condition used to wake up waiting getters when items arrive or when closed
        self._cond: asyncio.Condition = asyncio.Condition()

    def open(self) -> None:
        """Register a producer. Raises if the queue was already closed."""
        if self._closed:
            raise RuntimeError("Cannot open a closed queue")
        self._open_count += 1

    def close(self) -> None:
        """Producer calls this when it's done. If open-count reaches 0, close the queue."""
        if self._open_count > 0:
            self._open_count -= 1
        if self._open_count <= 0:
            # ensure we close when count drops to 0
            self._do_close()

    def _do_close(self) -> None:
        """Mark closed and wake waiting consumers."""
        if self._closed:
            return
        self._closed = True

        # notify waiting getters so they re-check the closed state
        async def _notify() -> None:
            async with self._cond:
                self._cond.notify_all()

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # not running in an event loop; nothing to notify
            return
        # schedule notify to run in loop
        loop.create_task(_notify())

    def close_force(self) -> None:
        """Force immediate close from outside (resets open count)."""
        self._open_count = 0
        self._do_close()

    @property
    def closed(self) -> bool:
        return self._closed

    async def put(self, item: T) -> None:
        """Put an item. Raises RuntimeError if queue is closed."""
        if self._closed:
            raise QueueClosed("Queue closed")
        await self._queue.put(item)
        # wake any waiting getters
        async with self._cond:
            self._cond.notify_all()

    def put_nowait(self, item: T) -> None:
        if self._closed:
            raise QueueClosed("Queue closed")
        self._queue.put_nowait(item)
        # try to notify condition (safe best-effort)
        try:
            loop = asyncio.get_running_loop()
            loop.call_soon_threadsafe(asyncio.create_task, self._notify_cond())
        except RuntimeError:
            # no running loop — it's okay
            pass

    async def _notify_cond(self) -> None:
        async with self._cond:
            self._cond.notify_all()

    async def get(self) -> T:
        """Get an item.

        Behavior:
        - If items are available, return one.
        - If no items but queue is not closed, wait until item or close.
        - If queue is closed and empty, raise QueueClosed.
        """
        while True:
            if not self._queue.empty():
                return await self._queue.get()
            if self._closed:
                raise QueueClosed("Queue closed and empty")
            # wait to be notified that an item arrived or the queue closed
            async with self._cond:
                await self._cond.wait()

    def qsize(self) -> int:
        return self._queue.qsize()

    def empty(self) -> bool:
        return self._queue.empty()

    def full(self) -> bool:
        return self._queue.full()

    async def join(self) -> None:
        await self._queue.join()

    def task_done(self) -> None:
        self._queue.task_done()

    def producer(self):
        """Return an async context manager for producers:

        async with q.producer():
            # inside: q.open() was called
            await q.put(...)
        # on exit: q.close() called
        """
        queue = self

        class _ProducerCtx:
            async def __aenter__(self):
                queue.open()
                return queue

            async def __aexit__(self, exc_type, exc, tb):
                queue.close()

        return _ProducerCtx()

    def __aiter__(self):
        # async iterator that yields until closed and drained
        return _AsyncClosableQueueIterator(self)


class _AsyncClosableQueueIterator:
    def __init__(self, q: AsyncClosableQueue):
        self._q = q

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            item = await self._q.get()
            return item
        except QueueClosed:
            raise StopAsyncIteration
