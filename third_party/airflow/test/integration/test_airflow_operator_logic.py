import os
import threading
import uuid
from typing import Any
from unittest.mock import MagicMock

import grpc
import pytest
from airflow.exceptions import AirflowException
from armada.model import GrpcChannelArgs
from armada.operators.armada import ArmadaOperator
from armada_client.armada import submit_pb2
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)
from armada_client.typings import JobState

DEFAULT_TASK_ID = "test_task_1"
DEFAULT_DAG_ID = "test_dag_1"
DEFAULT_RUN_ID = "test_run_1"
DEFAULT_QUEUE = "queue-a"
DEFAULT_NAMESPACE = "personal-anonymous"
DEFAULT_POLLING_INTERVAL = 1
DEFAULT_JOB_ACKNOWLEDGEMENT_TIMEOUT = 10


@pytest.fixture(scope="function", name="context")
def default_context() -> Any:
    mock_ti = MagicMock()
    mock_ti.task_id = DEFAULT_TASK_ID
    mock_ti.xcom_pull.return_value = None
    mock_ti.xcom_push.return_value = None
    mock_dag = MagicMock()
    mock_dag.dag_id = DEFAULT_DAG_ID
    return {
        "ti": mock_ti,
        "run_id": DEFAULT_RUN_ID,
        "dag": mock_dag,
    }


@pytest.fixture(scope="session", name="channel_args")
def queryapi_channel_args() -> GrpcChannelArgs:
    server_name = os.environ.get("ARMADA_SERVER", "localhost")
    server_port = os.environ.get("ARMADA_PORT", "50051")

    return GrpcChannelArgs(target=f"{server_name}:{server_port}")


@pytest.fixture(scope="session", name="client")
def no_auth_client() -> ArmadaClient:
    server_name = os.environ.get("ARMADA_SERVER", "localhost")
    server_port = os.environ.get("ARMADA_PORT", "50051")

    return ArmadaClient(channel=grpc.insecure_channel(f"{server_name}:{server_port}"))


def sleep_pod(image: str):
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="goodsleep",
                image=image,
                args=["sleep", "5s"],
                securityContext=core_v1.SecurityContext(runAsUser=1000),
                resources=core_v1.ResourceRequirements(
                    requests={
                        "cpu": api_resource.Quantity(string="0.2"),
                        "memory": api_resource.Quantity(string="64Mi"),
                    },
                    limits={
                        "cpu": api_resource.Quantity(string="0.2"),
                        "memory": api_resource.Quantity(string="64Mi"),
                    },
                ),
            )
        ],
    )
    return submit_pb2.JobSubmitRequestItem(
        priority=1, pod_spec=pod, namespace=DEFAULT_NAMESPACE
    )


def armada_operator(image: str, channel_args: GrpcChannelArgs):
    job_set_prefix = f"test-{uuid.uuid1()}"
    return ArmadaOperator(
        task_id=DEFAULT_TASK_ID,
        name="test_job_success",
        channel_args=channel_args,
        armada_queue=DEFAULT_QUEUE,
        job_request=sleep_pod(image),
        job_set_prefix=job_set_prefix,
        poll_interval=DEFAULT_POLLING_INTERVAL,
        job_acknowledgement_timeout=DEFAULT_JOB_ACKNOWLEDGEMENT_TIMEOUT,
        deferrable=False,
    )


def test_success_job(client: ArmadaClient, context: Any, channel_args: GrpcChannelArgs):
    operator = armada_operator("busybox", channel_args)

    operator.execute(context)
    job = operator.job_context

    assert job.state == JobState.SUCCEEDED
    response = client.get_job_status([job.job_id])
    assert JobState(response.job_states[job.job_id]) == JobState.SUCCEEDED


def test_bad_job(client: ArmadaClient, context: Any, channel_args: GrpcChannelArgs):
    operator = armada_operator("BADIMAGE", channel_args)

    try:
        operator.execute(context)
        pytest.fail(
            "Operator did not raise AirflowException on job failure as expected"
        )
    except AirflowException:  # Expected
        # Assert state failed
        job = operator.job_context
        assert job.state == JobState.FAILED

        # Assert actual job failed too
        response = client.get_job_status([job.job_id])
        assert JobState(response.job_states[job.job_id]) == JobState.FAILED
    except Exception as e:
        pytest.fail(
            "Operator did not raise AirflowException on job failure as expected, "
            f"raised {e} instead"
        )


# Used benchmark parallel execution below
def _success_job(
    task_number: int, context: Any, channel_args: GrpcChannelArgs, client: ArmadaClient
) -> JobState:
    operator = ArmadaOperator(
        task_id=f"{DEFAULT_TASK_ID}_{task_number}",
        name="test_job_success",
        channel_args=channel_args,
        armada_queue=DEFAULT_QUEUE,
        job_request=sleep_pod(image="busybox")[0],
        poll_interval=DEFAULT_POLLING_INTERVAL,
        job_acknowledgement_timeout=DEFAULT_JOB_ACKNOWLEDGEMENT_TIMEOUT,
        deferrable=False,
    )

    operator.execute(context)

    response = client.get_job_status([operator.job_id])
    return JobState(response.job_states[operator.job_id])


@pytest.mark.skip(reason="we should not test performance in the CI.")
def test_parallel_execution(
    client: ArmadaClient, context: Any, channel_args: GrpcChannelArgs, mocker
):
    threads = []
    _success_job(
        task_number=0, context=context, channel_args=channel_args, client=client
    )
    for task_number in range(5):
        t = threading.Thread(
            target=_success_job, args=[task_number, context, channel_args]
        )
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()


@pytest.mark.skip(reason="we should not test performance in the CI.")
def test_parallel_execution_large(
    client: ArmadaClient, context: Any, channel_args: GrpcChannelArgs, mocker
):
    threads = []
    _success_job(
        task_number=0, context=context, channel_args=channel_args, client=client
    )
    for task_number in range(80):
        t = threading.Thread(
            target=_success_job, args=[task_number, context, channel_args]
        )
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()


@pytest.mark.skip(reason="we should not test performance in the CI.")
def test_parallel_execution_huge(
    client: ArmadaClient, context: Any, channel_args: GrpcChannelArgs, mocker
):
    threads = []
    _success_job(
        task_number=0, context=context, channel_args=channel_args, client=client
    )
    for task_number in range(500):
        t = threading.Thread(
            target=_success_job, args=[task_number, context, channel_args]
        )
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()
