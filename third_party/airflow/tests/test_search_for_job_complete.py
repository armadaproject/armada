from typing import Tuple
from armada.operators.utils import search_for_job_complete


def test_failed_event():
    def job_failed_callable(
        queue: str, job_set_id: str, job_id: str
    ) -> Tuple[str, str]:
        return "Failed", "Test error message"

    job_complete = search_for_job_complete(
        airflow_task_name="test",
        job_id="id",
        queue="test",
        job_status_callable=job_failed_callable,
    )
    assert job_complete[0] == "Failed"
    assert job_complete[1] == "Test error message"
