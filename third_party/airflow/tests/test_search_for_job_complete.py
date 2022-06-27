from typing import Tuple
from armada.operators.utils import search_for_job_complete


def test_succeded_event():
    event_stream_message = event_pb2.EventStreamMessage()
    event_message = event_pb2.EventMessage()
    event_stream_message.id = "test"

    successful_event = event_pb2.JobSucceededEvent(job_id="id")
    event_message.succeeded.CopyFrom(successful_event)
    event_stream_message.message.CopyFrom(event_message)

    # We are constructing a EventMessage and creating a list out of it
    # This is to mock a grpc stream.
    # IRL this would be given to us from a grpc call
    events = [event_stream_message]
    job_complete = search_for_job_complete(events, job_name="test", job_id="id")
    assert job_complete[0] == "successful"
    assert job_complete[1] == "Armada test:id succeeded"


def test_failed_event():
    def job_failed_callable(queue: str, job_set_id: str, job_id: str) -> Tuple[str, str]:
        return "Failed", "Test error message"
    job_complete = search_for_job_complete(airflow_task_name="test", job_id="id", queue='test', job_status_callable=job_failed_callable)
    assert job_complete[0] == "Failed"
    assert job_complete[1] == "Test error message"


