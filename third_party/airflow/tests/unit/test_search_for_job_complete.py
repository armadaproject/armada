
from armada.operators.utils import search_for_job_complete
from armada.jobservice import jobservice_pb2


def test_failed_event():
    def test_callable(queue: str, job_set_id: str, job_id: str):
        return jobservice_pb2.JobServiceResponse(
            state=jobservice_pb2.JobServiceResponse.FAILED, error="Testing Failure"
        )

    job_complete = search_for_job_complete(
        airflow_task_name="test",
        job_id="id",
        queue="test",
        job_set_id="test",
        job_status_callable=test_callable,
    )
    assert job_complete[0] == "failed"
    assert (
        job_complete[1] == "Armada test:id failed\nfailed with reason Testing Failure"
    )


def test_successful_event():
    def test_callable(queue: str, job_set_id: str, job_id: str):
        return jobservice_pb2.JobServiceResponse(state=jobservice_pb2.JobServiceResponse.SUCCEEDED)

    job_complete = search_for_job_complete(
        airflow_task_name="test",
        job_id="id",
        queue="test",
        job_set_id="test",
        job_status_callable=test_callable,
    )
    assert job_complete[0] == "succeeded"
    assert job_complete[1] == "Armada test:id succeeded"

def test_cancelled_event():
    def test_callable(queue: str, job_set_id: str, job_id: str):
        return jobservice_pb2.JobServiceResponse(state=jobservice_pb2.JobServiceResponse.CANCELLED)

    job_complete = search_for_job_complete(
        airflow_task_name="test",
        job_id="id",
        queue="test",
        job_set_id="test",
        job_status_callable=test_callable,
    )
    assert job_complete[0] == "cancelled"
    assert job_complete[1] == "Armada test:id cancelled"
