from armada.operators.armada import ArmadaOperator
from armada.job_service import JobService

import copy
from unittest.mock import patch, Mock

import grpc
import pytest
import unittest
from unittest.mock import patch


from armada.jobservice import jobservice_pb2
from armada.operators.armada import ArmadaOperator
from armada.operators.grpc import CredentialsCallback
from armada.operators.utils import JobState

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

def test_deepcopy_operator():
    armada_channel_args = {"target": "127.0.0.1:50051"}
    job_service_channel_args = {"target": "127.0.0.1:60003"}

    operator = ArmadaOperator(
        task_id="test_task_id",
        name="test_task",
        armada_channel_args=armada_channel_args,
        job_service_channel_args=job_service_channel_args,
        armada_queue="test_queue",
        job_request_items=[],
        lookout_url_template="http://localhost:8089/jobs?job_id=<job_id>",
    )

    try:
        copy.deepcopy(operator)
    except Exception as e:
        assert False, f"{e}"


@pytest.mark.skip("demonstrates how the old way of passing in credentials fails")
def test_deepcopy_operator_with_grpc_credentials():
    armada_channel_args = {
        "target": "127.0.0.1:50051",
        "credentials": grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(),
            grpc.metadata_call_credentials(("authorization", "fake_jwt")),
        ),
    }
    job_service_channel_args = {"target": "127.0.0.1:60003"}

    operator = ArmadaOperator(
        task_id="test_task_id",
        name="test_task",
        armada_channel_args=armada_channel_args,
        job_service_channel_args=job_service_channel_args,
        armada_queue="test_queue",
        job_request_items=[],
        lookout_url_template="http://localhost:8089/jobs?job_id=<job_id>",
    )

    try:
        copy.deepcopy(operator)
    except Exception as e:
        assert False, f"{e}"


def test_deepcopy_operator_with_grpc_credentials_callback():
    armada_channel_args = {
        "target": "127.0.0.1:50051",
        "credentials_callback_args": {
            "module_name": "tests.unit.test_armada_operator",
            "function_name": "__example_test_callback",
            "function_kwargs": {
                "test_arg": "fake_arg",
            },
        },
    }
    job_service_channel_args = {"target": "127.0.0.1:60003"}

    operator = ArmadaOperator(
        task_id="test_task_id",
        name="test_task",
        armada_channel_args=armada_channel_args,
        job_service_channel_args=job_service_channel_args,
        armada_queue="test_queue",
        job_request_items=[],
        lookout_url_template="http://localhost:8089/jobs?job_id=<job_id>",
    )

    try:
        copy.deepcopy(operator)
    except Exception as e:
        assert False, f"{e}"


def __example_test_callback(foo=None):
    return f"fake_cred {foo}"


def test_credentials_callback():
    callback = CredentialsCallback(
        module_name="test_armada_operator",
        function_name="__example_test_callback",
        function_kwargs={"foo": "bar"},
    )

    result = callback.call()
    assert result == "fake_cred bar"


@patch("armada.operators.armada.search_for_job_complete")
@patch("armada.operators.armada.ArmadaClient", autospec=True)
@patch("armada.operators.armada.JobServiceClient", autospec=True)
def test_armada_operator_execute(
    JobServiceClientMock, ArmadaClientMock, search_for_job_complete_mock
):
    jsclient_mock = Mock()
    jsclient_mock.health.return_value = jobservice_pb2.HealthCheckResponse(
        status=jobservice_pb2.HealthCheckResponse.SERVING
    )

    JobServiceClientMock.return_value = jsclient_mock

    item = Mock()
    item.job_id = "fake_id"

    job = Mock()
    job.job_response_items = [
        item,
    ]

    aclient_mock = Mock()
    aclient_mock.submit_jobs.return_value = job
    ArmadaClientMock.return_value = aclient_mock

    search_for_job_complete_mock.return_value = (JobState.SUCCEEDED, "No error")

    armada_channel_args = {"target": "127.0.0.1:50051"}
    job_service_channel_args = {"target": "127.0.0.1:60003"}

    operator = ArmadaOperator(
        task_id="test_task_id",
        name="test_task",
        armada_channel_args=armada_channel_args,
        job_service_channel_args=job_service_channel_args,
        armada_queue="test_queue",
        job_request_items=[],
        lookout_url_template="https://lookout.armada.domain/jobs?job_id=<job_id>",
    )

    task_instance = Mock()
    task_instance.task_id = "mock_task_id"

    dag = Mock()
    dag.dag_id = "mock_dag_id"

    context = {
        "run_id": "mock_run_id",
        "ti": task_instance,
        "dag": dag,
    }

    try:
        operator.execute(context)
    except Exception as e:
        assert False, f"{e}"

    jsclient_mock.health.assert_called()
    aclient_mock.submit_jobs.assert_called()

