import os
import uuid
import pytest
import threading

from armada_client.armada import (
    submit_pb2,
)
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)
import grpc

from armada.operators.jobservice import JobServiceClient
from armada.operators.utils import JobState, search_for_job_complete


@pytest.fixture(scope="session", name="jobservice")
def job_service_client() -> ArmadaClient:
    server_name = os.environ.get("JOB_SERVICE_HOST", "localhost")
    server_port = os.environ.get("JOB_SERVICE_PORT", "60003")

    return JobServiceClient(
        channel=grpc.insecure_channel(f"{server_name}:{server_port}")
    )


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
                args=["sleep", "10s"],
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
    return [
        submit_pb2.JobSubmitRequestItem(
            priority=1, pod_spec=pod, namespace="personal-anonymous"
        )
    ]


def test_success_job(client: ArmadaClient, jobservice: JobServiceClient):
    job_set_name = f"test-{uuid.uuid1()}"
    job = client.submit_jobs(
        queue="queue-a",
        job_set_id=job_set_name,
        job_request_items=sleep_pod(image="busybox"),
    )
    job_id = job.job_response_items[0].job_id

    job_state, job_message = search_for_job_complete(
        job_service_client=jobservice,
        armada_queue="queue-a",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=job_id,
    )
    assert job_state == JobState.SUCCEEDED
    assert job_message == f"Armada test:{job_id} succeeded"


def test_bad_job(client: ArmadaClient, jobservice: JobServiceClient):
    job_set_name = f"test-{uuid.uuid1()}"

    job = client.submit_jobs(
        queue="queue-a",
        job_set_id=job_set_name,
        job_request_items=sleep_pod(image="NOTACONTAINER"),
    )
    job_id = job.job_response_items[0].job_id

    job_state, job_message = search_for_job_complete(
        job_service_client=jobservice,
        armada_queue="queue-a",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=job_id,
    )
    assert job_state == JobState.FAILED
    assert job_message.startswith(f"Armada test:{job_id} failed")


def test_two_jobs(client: ArmadaClient, jobservice: JobServiceClient):
    job_set_name = f"test-{uuid.uuid1()}"

    first_job = client.submit_jobs(
        queue="queue-a",
        job_set_id=job_set_name,
        job_request_items=sleep_pod(image="busybox"),
    )
    first_job_id = first_job.job_response_items[0].job_id

    job_state, job_message = search_for_job_complete(
        job_service_client=jobservice,
        armada_queue="queue-a",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=first_job_id,
    )
    assert job_state == JobState.SUCCEEDED
    assert job_message == f"Armada test:{first_job_id} succeeded"

    second_job = client.submit_jobs(
        queue="queue-a",
        job_set_id=job_set_name,
        job_request_items=sleep_pod(image="busybox"),
    )
    second_job_id = second_job.job_response_items[0].job_id

    job_state, job_message = search_for_job_complete(
        job_service_client=jobservice,
        armada_queue="queue-a",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=second_job_id,
    )
    assert job_state == JobState.SUCCEEDED
    assert job_message == f"Armada test:{second_job_id} succeeded"


def test_two_jobs_good_bad(client: ArmadaClient, jobservice: JobServiceClient):
    job_set_name = f"test-{uuid.uuid1()}"

    first_job = client.submit_jobs(
        queue="queue-a",
        job_set_id=job_set_name,
        job_request_items=sleep_pod(image="busybox"),
    )
    first_job_id = first_job.job_response_items[0].job_id

    job_state, job_message = search_for_job_complete(
        job_service_client=jobservice,
        armada_queue="queue-a",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=first_job_id,
    )
    assert job_state == JobState.SUCCEEDED
    assert job_message == f"Armada test:{first_job_id} succeeded"

    second_job = client.submit_jobs(
        queue="queue-a",
        job_set_id=job_set_name,
        job_request_items=sleep_pod(image="NOTACONTAINER"),
    )
    second_job_id = second_job.job_response_items[0].job_id

    job_state, job_message = search_for_job_complete(
        job_service_client=jobservice,
        armada_queue="queue-a",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=second_job_id,
    )
    assert job_state == JobState.FAILED
    assert job_message.startswith(f"Armada test:{second_job_id} failed")

def success_job(client: ArmadaClient, jobservice: JobServiceClient):
    job_set_name = f"test-{uuid.uuid1()}"
    job = client.submit_jobs(
        queue="queue-a",
        job_set_id=job_set_name,
        job_request_items=sleep_pod(image="busybox"),
    )
    job_id = job.job_response_items[0].job_id

    job_state, job_message = search_for_job_complete(
        job_service_client=jobservice,
        armada_queue="queue-a",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=job_id,
    )

    assert job_state == JobState.SUCCEEDED
    assert job_message == f"Armada test:{first_job_id} succeeded"


def test_parallel_execution(client: ArmadaClient, jobservice: JobServiceClient):
    threads = []
    for _ in range(20):
        t = threading.Thread(target=success_job, args=[client, jobservice])
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()