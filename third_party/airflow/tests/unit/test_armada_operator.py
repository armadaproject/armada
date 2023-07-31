import copy

import grpc
import pytest

from armada.operators.armada import ArmadaOperator
from armada.operators.grpc import CredentialsCallback

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
        module_name="tests.unit.test_armada_operator",
        function_name="__example_test_callback",
        function_kwargs={"foo": "bar"},
    )

    result = callback.call()
    assert result == "fake_cred bar"
