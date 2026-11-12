import logging
from typing import List, Optional, Tuple
from unittest.mock import MagicMock

import pytest
from armada.links import UrlFromLogsExtractor
from armada.log_manager import KubernetesPodLogManager
from armada.model import LogPosition
from pendulum import UTC, DateTime

CONTAINER = "main"
T1 = DateTime(2026, 1, 1, 12, 0, 1, tzinfo=UTC)
T2 = DateTime(2026, 1, 1, 12, 0, 2, tzinfo=UTC)
T3 = DateTime(2026, 1, 1, 12, 0, 3, tzinfo=UTC)


class FakePodLogResponse:
    """The body of a kubelet log response with timestamps=True."""

    def __init__(self, records: List[Tuple[DateTime, str]]):
        self._body = "".join(f"{ts.isoformat()} {message}\n" for ts, message in records)

    def read(self) -> bytes:
        return self._body.encode("utf-8")


@pytest.fixture
def manager() -> KubernetesPodLogManager:
    manager = KubernetesPodLogManager()
    # Airflow 3 logs through structlog, which caplog cannot see; LoggingMixin uses _log.
    manager._log = logging.getLogger("test.log_manager")
    return manager


def poll(
    manager: KubernetesPodLogManager,
    caplog: pytest.LogCaptureFixture,
    since: Optional[LogPosition],
    records: List[Tuple[DateTime, str]],
) -> Tuple[Optional[LogPosition], List[str]]:
    """One operator poll: returns the new position and the container messages logged."""
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="test.log_manager"):
        position = manager._stream_logs(
            CONTAINER,
            since,
            FakePodLogResponse(records),
            UrlFromLogsExtractor({}, MagicMock()),
        )
    prefix = f"[{CONTAINER}] "
    messages = [
        r.getMessage().removeprefix(prefix)
        for r in caplog.records
        if r.getMessage().startswith(prefix)
    ]
    return position, messages


def test_carriage_return_record_returned_again_is_not_logged_again(manager, caplog):
    record = [(T1, "1\r2\r3")]

    position, first = poll(manager, caplog, None, record)
    position, second = poll(manager, caplog, position, record)
    position, third = poll(manager, caplog, position, record)

    assert first == ["1\n2\n3"]
    assert second == []
    assert third == []
    assert position == LogPosition(T1, len("1\r2\r3"))


def test_growing_carriage_return_record_logs_only_new_fragments(manager, caplog):
    position, first = poll(manager, caplog, None, [(T1, "1\r2\r3")])
    position, second = poll(manager, caplog, position, [(T1, "1\r2\r3\r4")])
    position, third = poll(manager, caplog, position, [(T1, "1\r2\r3\r4\r5")])

    assert [first, second, third] == [["1\n2\n3"], ["4"], ["5"]]


def test_record_after_a_carriage_return_record_is_logged_once(manager, caplog):
    position, first = poll(manager, caplog, None, [(T1, "1\r2\r3")])
    position, second = poll(manager, caplog, position, [(T1, "1\r2\r3"), (T2, "done")])
    position, third = poll(manager, caplog, position, [(T2, "done")])

    assert [first, second, third] == [["1\n2\n3"], ["done"], []]
    assert position == LogPosition(T2, len("done"))


def test_newline_delimited_records_are_each_logged_once(manager, caplog):
    position, first = poll(manager, caplog, None, [(T1, "a"), (T2, "b")])
    position, second = poll(manager, caplog, position, [(T2, "b"), (T3, "c")])

    assert first == ["a", "b"]
    assert second == ["c"]
    assert position == LogPosition(T3, len("c"))


def test_no_new_records_keeps_the_position(manager, caplog):
    since = LogPosition(T2, len("b"))

    position, messages = poll(manager, caplog, since, [(T1, "a"), (T2, "b")])

    assert messages == []
    assert position == since
