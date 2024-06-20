import asyncio
import datetime
import time
import pytest

from armada_client.iterators import (
    AsyncTimeoutIterator,
    IteratorTimeoutException,
    TimeoutIterator,
)


def test_timeout_iterator_propagates_results():
    iterator = iter([1, 2, 3])
    timeout_iterator = TimeoutIterator[int](
        iterator, timeout=datetime.timedelta(seconds=1)
    )
    assert list(timeout_iterator) == [1, 2, 3]


def test_timeout_iterator_propagates_underlying_error():
    def error_generator():
        yield 1
        raise ValueError("Error")

    timeout_iterator = TimeoutIterator[int](
        error_generator(), timeout=datetime.timedelta(seconds=1)
    )

    with pytest.raises(ValueError):
        list(timeout_iterator)


def test_timeout_iterator_raises_timeout_error():
    def timeout_generator():
        yield 1
        time.sleep(60)

    timeout_iterator = TimeoutIterator[int](
        timeout_generator(), timeout=datetime.timedelta(milliseconds=1)
    )

    assert next(timeout_iterator) == 1
    with pytest.raises(IteratorTimeoutException):
        next(timeout_iterator)


def test_timeout_iterator_stop_stops_iteration_without_error():
    def generator():
        yield 1

    timeout_iterator = TimeoutIterator[int](
        generator(),
        timeout=datetime.timedelta(seconds=1),
    )
    timeout_iterator.stop()

    assert list(timeout_iterator) == []


async def test_async_timeout_iterator_propagates_results():
    data = [1, 2, 3]

    async def aiter():
        for item in data:
            yield item

    timeout_iterator = AsyncTimeoutIterator[int](
        aiter(), timeout=datetime.timedelta(seconds=1)
    )
    assert [i async for i in timeout_iterator] == [1, 2, 3]


async def test_async_timeout_iterator_propagates_underlying_error():
    async def error_generator():
        yield 1
        raise ValueError("Error")

    timeout_iterator = AsyncTimeoutIterator[int](
        error_generator(), timeout=datetime.timedelta(seconds=1)
    )

    with pytest.raises(ValueError):
        [i async for i in timeout_iterator]


async def test_async_timeout_iterator_raises_timeout_error():
    async def timeout_generator():
        yield 1
        await asyncio.sleep(60)

    timeout_iterator = AsyncTimeoutIterator[int](
        timeout_generator(), timeout=datetime.timedelta(milliseconds=1)
    )

    assert await timeout_iterator.__anext__() == 1
    with pytest.raises(IteratorTimeoutException):
        await timeout_iterator.__anext__()


async def test_async_timeout_iterator_stop_stops_iteration_without_error():
    async def generator():
        yield 1

    timeout_iterator = AsyncTimeoutIterator[int](
        generator(),
        timeout=datetime.timedelta(seconds=1),
    )
    timeout_iterator.stop()

    assert [i async for i in timeout_iterator] == []
