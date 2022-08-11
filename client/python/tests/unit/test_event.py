import pytest

from armada_client.client import ArmadaClient
from armada_client.event import Event
from armada_client.typings import EventType


class FakeEvent:
    def __init__(self):
        self.job_id = "job-1"
        self.job_set_id = "job-set-1"
        self.queue = "queue-1"


class FakeEventMessage:
    def __init__(self, name):
        self.fake_name = name
        setattr(self, name, FakeEvent())

    def WhichOneof(self, _):
        return self.fake_name


class FakeEventStreamMessage:
    def __init__(self, name):
        self.message = FakeEventMessage(name)
        self.id = 1


@pytest.mark.parametrize(
    "name, event_type",
    [
        ("submitted", EventType.submitted),
        ("queued", EventType.queued),
        ("duplicate_found", EventType.duplicate_found),
        ("leased", EventType.leased),
        ("lease_returned", EventType.lease_returned),
        ("lease_expired", EventType.lease_expired),
        ("pending", EventType.pending),
        ("running", EventType.running),
        ("unable_to_schedule", EventType.unable_to_schedule),
        ("failed", EventType.failed),
        ("succeeded", EventType.succeeded),
        ("reprioritized", EventType.reprioritized),
        ("cancelling", EventType.cancelling),
        ("cancelled", EventType.cancelled),
        ("terminated", EventType.terminated),
        ("utilisation", EventType.utilisation),
        ("ingress_info", EventType.ingress_info),
        ("reprioritizing", EventType.reprioritizing),
        ("updated", EventType.updated),
        ("failedCompressed", EventType.failedCompressed),
    ],
)
def test_event_class(name, event_type):
    test_event = FakeEventStreamMessage(name)
    test_event = Event(test_event)

    assert test_event.id == 1
    assert test_event.type == event_type
    assert test_event.message.job_id == "job-1"
    assert test_event.message.job_set_id == "job-set-1"
    assert test_event.message.queue == "queue-1"


@pytest.mark.parametrize(
    "name",
    [
        "submitted",
        "queued",
        "duplicate_found",
        "leased",
        "lease_returned",
        "lease_expired",
        "pending",
        "running",
        "unable_to_schedule",
        "failed",
        "succeeded",
        "reprioritized",
        "cancelling",
        "cancelled",
        "terminated",
        "utilisation",
        "ingress_info",
        "reprioritizing",
        "updated",
        "failedCompressed",
    ],
)
def test_unmarshal_event_response(name):
    test_event = FakeEventStreamMessage(name)
    test_event_from_class = Event(test_event)

    test_event_from_method = ArmadaClient.unmarshal_event_response(test_event)

    assert test_event_from_method.id == test_event_from_class.id
    assert test_event_from_method.type == test_event_from_class.type
    assert test_event_from_method.message.job_id == test_event_from_class.message.job_id
    assert (
        test_event_from_method.message.job_set_id
        == test_event_from_class.message.job_set_id
    )
    assert test_event_from_method.message.queue == test_event_from_class.message.queue
