import copy

import pytest

from armada_client.armada import submit_pb2
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)
from armada.operators.armada_deferrable import ArmadaDeferrableOperator
from armada.operators.grpc import CredentialsCallback


def test_serialize_armada_deferrable():
    grpc_chan_args = {
        "target": "localhost:443",
        "credentials_callback_args": {
            "module_name": "channel_test",
            "function_name": "get_credentials",
            "function_kwargs": {
                "example_arg": "test",
            },
        },
    }

    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="sleep",
                image="busybox",
                args=["sleep", "10s"],
                securityContext=core_v1.SecurityContext(runAsUser=1000),
                resources=core_v1.ResourceRequirements(
                    requests={
                        "cpu": api_resource.Quantity(string="120m"),
                        "memory": api_resource.Quantity(string="510Mi"),
                    },
                    limits={
                        "cpu": api_resource.Quantity(string="120m"),
                        "memory": api_resource.Quantity(string="510Mi"),
                    },
                ),
            )
        ],
    )

    job_requests = [
        submit_pb2.JobSubmitRequestItem(
            priority=1,
            pod_spec=pod,
            namespace="personal-anonymous",
            annotations={"armadaproject.io/hello": "world"},
        )
    ]

    source = ArmadaDeferrableOperator(
        task_id="test_task_id",
        name="test task",
        armada_channel_args=grpc_chan_args,
        job_service_channel_args=grpc_chan_args,
        armada_queue="test-queue",
        job_request_items=job_requests,
        lookout_url_template="https://lookout.test.domain/",
        poll_interval=5,
    )

    serialized = source.serialize()
    assert serialized["name"] == source.name

    reconstituted = ArmadaDeferrableOperator(**serialized)
    assert reconstituted == source


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

    operator = ArmadaDeferrableOperator(
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

    operator = ArmadaDeferrableOperator(
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

    operator = ArmadaDeferrableOperator(
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
