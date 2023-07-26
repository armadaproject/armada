from armada.operators.armada import ArmadaOperator
import pytest

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
    armada_channel_args = {"target": "127.0.0.1:50051"}
    job_service_channel_args = {"target": "127.0.0.1:60003"}

    operator = ArmadaOperator(
        task_id="test_task_id",
        name="test_task",
        armada_channel_args=armada_channel_args,
        job_service_channel_args=job_service_channel_args,
        armada_queue="test_queue",
        job_request_items=[],
        lookout_url_template=lookout_url_template,
    )

    assert operator._get_lookout_url(job_id) == expected_url
