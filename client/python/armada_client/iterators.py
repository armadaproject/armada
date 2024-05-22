# Internal iterators with timeout, to detect client disconnection
from __future__ import annotations

import datetime
import queue
import asyncio
import threading
from typing import (
    Any,
    AsyncIterator,
    Iterator,
    Optional,
    Type,
    TypeVar,
    Generic,
)


class IteratorTimeoutException(Exception):
    def __init__(self) -> None:
        super().__init__("Timeout while waiting for next element in iterator")


T = TypeVar("T")


def _utcnow():
    # It's a method, so we can mock out time, if we need to.
    return datetime.datetime.now(datetime.timezone.utc)


class _PollerMixIn(Generic[T]):
    def __init__(
        self,
        timeout: datetime.timedelta,
        poll_interval: datetime.timedelta,
        stop_termination_exception: Type[Exception],
    ) -> None:
        self._timeout = timeout
        self._poll_interval = poll_interval
        self._stop_termination_exception = stop_termination_exception
        self._done = False

    def stop(self):
        """
        Mark underlying thread for termination (on next result being yielded).
        Immediately stop consuming the iterator.
        """
        self._done = True

    def _should_keep_polling(
        self, started_at: datetime.datetime, data: Optional[Any]
    ) -> bool:
        """
        Should iterator keep polling for new data.
        """
        timed_out = _utcnow() - started_at > self._timeout
        should_terminate = data is not None or self._done or timed_out
        return not should_terminate

    def _handle_result(self, data: Optional[T]) -> T:
        """
        Propagate result from underlying iterator, raise exceptions if any.
        """

        # propagate any exceptions including StopIteration
        if isinstance(data, BaseException):
            self._done = True
            raise data
        # raise timeout exception if no data was received
        if data is None and not self._done:
            raise IteratorTimeoutException()

        if data is None:
            raise self._stop_termination_exception()

        return data


class TimeoutIterator(Generic[T], _PollerMixIn[T], Iterator[T]):
    """
    INTERNAL (no compatibility guarantees between library releases).

    Wraps an iterator with timeout functionality.
    Raises IteratorTimeoutException if timeout.
    Uses background thread, to implement timeout functionality.

    :param iterator: iterator to wrap
    :param timeout: timeout duration
    :param poll_interval: interval to poll for new data
    """

    def __init__(
        self,
        iterator: Iterator[T],
        timeout: datetime.timedelta,
        poll_interval: datetime.timedelta = datetime.timedelta(seconds=1),
    ):
        _PollerMixIn.__init__(self, timeout, poll_interval, StopIteration)
        self._iterator = iterator
        self._buffer = queue.Queue()
        self._thread = threading.Thread(target=self.__consume, daemon=True)
        self._thread.start()

    def __iter__(self) -> TimeoutIterator[T]:
        return self

    def __next__(self) -> T:
        """
        Yields result from underlying iterator.
        Raises IteratorTimeoutException if timeout.
        """
        now = _utcnow()
        data = None
        while self._should_keep_polling(now, data):
            try:
                data = self._buffer.get(timeout=self._poll_interval.total_seconds())
            except queue.Empty:
                pass

        return self._handle_result(data)

    def __consume(self):
        """
        Consume from underlying iterator and put data
        (including any exceptions) into buffer.
        """
        try:
            for data in self._iterator:
                self._buffer.put(data)
                if self._done:
                    break
            self._buffer.put(self._stop_termination_exception())
        except BaseException as e:
            self._buffer.put(e)


class AsyncTimeoutIterator(Generic[T], _PollerMixIn, AsyncIterator[T]):
    """
    INTERNAL (no compatibility guarantees between library releases).

    Wraps an async iterator with timeout functionality.
    Raises IteratorTimeoutException if timeout.

    :param iterator: iterator to wrap
    :param timeout: timeout duration
    :param poll_interval: interval to poll for new data
    """

    def __init__(
        self,
        iterator: AsyncIterator[T],
        timeout: datetime.timedelta,
        poll_interval: datetime.timedelta = datetime.timedelta(seconds=1),
    ):
        _PollerMixIn.__init__(self, timeout, poll_interval, StopAsyncIteration)
        self._iterator = iterator
        self._buffer = asyncio.Queue()
        self._task = asyncio.get_event_loop().create_task(self.__consume())

    def __aiter__(self):
        return self

    async def __anext__(self):
        """
        Yields result from underlying iterator.
        Raises IteratorTimeoutException if timeout.
        """

        now = _utcnow()
        data = None
        while self._should_keep_polling(now, data):
            try:
                data = await asyncio.wait_for(
                    self._buffer.get(), self._timeout.total_seconds()
                )
            except asyncio.TimeoutError:
                pass

        return self._handle_result(data)

    async def __consume(self):
        """
        Consume from underlying iterator and put data
        (including any exceptions) into buffer.
        """
        try:
            async for data in self._iterator:
                await self._buffer.put(data)
                if self._done:
                    break
            await self._buffer.put(self._stop_termination_exception())
        except BaseException as e:
            await self._buffer.put(e)
