from armada.operators.armada import ArmadaOperator
from armada_client import cancel_jobset

import pytest
import unittest
import armada_client

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

    def test_cancel_jobs(self, job_id, queue):
        # Set the job_set_id and queue
        self.job_set_id = job_id
        self.queue = queue

        # Call the on_kill function
        self.on_kill()

        # Cancel the jobs
        response = self.armada_client.cancel_jobset(queue=self.queue, job_set_id=self.job_set_id)

        # Assert that the jobs were successfully canceled
        self.assertEqual(response, empty_pb2.Empty())

if __name__ == "__main__":
    unittest.main()
