from armada.operators.armada import ArmadaOperator
from armada.job_service import JobService

import pytest
import unittest
from unittest.mock import patch


get_lookout_url_test_cases = [
    (
        "http://localhost:8089/jobs?job_id=<job_id>",
        "test_id",
        "http://localhost:8089/jobs?job_id=test_id",
    ),
    (
        "https://lookout.armada.domain/jobs?job_id=<job_id>",
        "test_id",
        "https://lookout.armada.domain/jobs?job_id=test_id",
    ),
    ("", "test_id", ""),
    (None, "test_id", ""),
]


@pytest.mark.parametrize(
    "lookout_url_template, job_id, expected_url", get_lookout_url_test_cases
)
def test_get_lookout_url(lookout_url_template, job_id, expected_url):
    operator = ArmadaOperator(
        task_id="test_task_id",
        name="test_task",
        armada_client=None,
        job_service_client=None,
        armada_queue="test_queue",
        job_request_items=[],
        lookout_url_template=lookout_url_template,
    )

    assert operator._get_lookout_url(job_id) == expected_url


class TestJobService(unittest.TestCase):
    @patch.object(JobService, "cancel_jobs")
    def test_on_kill(self, mock_cancel_jobs):
        mock_cancel_jobs.return_value = None

        job_service = JobService(job_set_id="test_job_set_id", queue="test_queue")

        job_service.on_kill()

        mock_cancel_jobs.assert_called_once_with(
            job_set_id="test_job_set_id", queue="test_queue"
        )


if __name__ == "__main__":
    unittest.main()
