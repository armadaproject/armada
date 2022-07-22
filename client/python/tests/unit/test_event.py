from armada_client.event import Event

from armada_client.typings import EventType


class FakeSubmittedEvent:
    def __init__(self):
        self.job_id = "job-1"
        self.job_set_id = "job-set-1"
        self.queue = "queue-1"


class FakeEventMessage:
    def __init__(self):
        self.submitted = FakeSubmittedEvent()

    def WhichOneof(self, _):
        return EventType.submitted.value


class FakeEventStreamMessage:
    def __init__(self):
        self.message = FakeEventMessage()
        self.id = 1


def test_event_class():
    test_event = FakeEventStreamMessage()
    test_event = Event(test_event)

    assert test_event.id == 1
    assert test_event.type == EventType.submitted
    assert test_event.message.job_id == "job-1"
    assert test_event.message.job_set_id == "job-set-1"
    assert test_event.message.queue == "queue-1"
