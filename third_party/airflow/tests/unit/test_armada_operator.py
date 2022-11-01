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

    assert operator.get_lookout_url(job_id) == expected_url
