from armada_client.armada import event_pb2
from armada_client.client import search_for_job_complete


def test_succeded_event():
    event_stream_message = event_pb2.EventStreamMessage()
    event_message = event_pb2.EventMessage()
    event_stream_message.id = "test"

    successful_event = event_pb2.JobSucceededEvent(job_id="id")
    event_message.succeeded.CopyFrom(successful_event)
    event_stream_message.message.CopyFrom(event_message)

    # We are constructing a EventMessage and creating a list out of it
    # This is to mock a grpc stream.
    # In a real use case, this is a generator, not a list
    events = [event_stream_message]
    job_complete = search_for_job_complete(events, job_name="test", job_id="id")
    assert job_complete[0] == "successful"
    assert job_complete[1] == "Armada test:id succeeded"


def test_failed_event():
    event_stream_message = event_pb2.EventStreamMessage()
    event_message = event_pb2.EventMessage()
    event_stream_message.id = "test"

    failed_event = event_pb2.JobFailedEvent(job_id="id", reason="test fail")
    event_message.failed.CopyFrom(failed_event)
    event_stream_message.message.CopyFrom(event_message)

    events = [event_stream_message]
    job_complete = search_for_job_complete(events, job_name="test", job_id="id")
    assert job_complete[0] == "failed"
    assert job_complete[1] == "Armada test:id failed\nfailed with reason test fail"


def test_cancelling_event():
    event_stream_message = event_pb2.EventStreamMessage()
    event_message = event_pb2.EventMessage()
    event_stream_message.id = "test"

    cancelling_event = event_pb2.JobCancellingEvent(job_id="id")
    event_message.cancelling.CopyFrom(cancelling_event)
    event_stream_message.message.CopyFrom(event_message)

    events = [event_stream_message]
    job_complete = search_for_job_complete(events, job_name="test", job_id="id")
    assert job_complete[0] == "cancelling"
    assert job_complete[1] == "Armada test:id cancelling"


def test_cancelled_event():
    event_stream_message = event_pb2.EventStreamMessage()
    event_message = event_pb2.EventMessage()
    event_stream_message.id = "test"

    cancelled_event = event_pb2.JobCancelledEvent(job_id="id")
    event_message.cancelled.CopyFrom(cancelled_event)
    event_stream_message.message.CopyFrom(event_message)

    events = [event_stream_message]
    job_complete = search_for_job_complete(events, job_name="test", job_id="id")
    assert job_complete[0] == "cancelled"
    assert job_complete[1] == "Armada test:id cancelled"


def test_terminated_event():
    event_stream_message = event_pb2.EventStreamMessage()
    event_message = event_pb2.EventMessage()
    event_stream_message.id = "test"

    terminated_event = event_pb2.JobTerminatedEvent(job_id="id")
    event_message.terminated.CopyFrom(terminated_event)
    event_stream_message.message.CopyFrom(event_message)

    events = [event_stream_message]
    job_complete = search_for_job_complete(events, job_name="test", job_id="id")
    assert job_complete[0] == "terminated"
    assert job_complete[1] == "Armada test:id terminated"
