import uuid
from armada_client.armada import (
    submit_pb2,
)
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)
import grpc
import time
import os
import pytest


def client() -> ArmadaClient:
    server_name, server_port = "localhost", "50051"
    if os.environ.get("ARMADA_SERVER"):
        server_name = os.environ.get("ARMADA_SERVER")

    if os.environ.get("ARMADA_PORT"):
        server_port = os.environ.get("ARMADA_PORT")

    return ArmadaClient(
        channel=grpc.insecure_channel(target=f"{server_name}:{server_port}")
    )


no_auth_client = client()


def sleep(sleep_time):
    time.sleep(sleep_time)


queue_name = f"queue-{uuid.uuid1()}"


@pytest.fixture(scope="session", autouse=True)
def queue():

    no_auth_client.create_queue(name=queue_name, priority_factor=1)
    sleep(3)


def test_get_queue():
    queue = no_auth_client.get_queue(name=queue_name)
    assert queue.name == queue_name


def test_get_queue_info():
    queue = no_auth_client.get_queue_info(name=queue_name)
    assert queue.name == queue_name
    assert not queue.active_job_sets


def test_submit_job_and_cancel_by_id():
    job_set_name = f"set-{uuid.uuid1()}"
    sleep(5)
    jobs = no_auth_client.submit_jobs(
        queue=queue_name, job_set_id=job_set_name, job_request_items=submit_sleep_job()
    )
    # Jobs can must be finished before deleting queue
    cancel_response = no_auth_client.cancel_jobs(
        job_id=jobs.job_response_items[0].job_id
    )
    assert cancel_response.cancelled_ids[0] == jobs.job_response_items[0].job_id


def test_submit_job_and_cancel_by_queue_job_set():
    job_set_name = f"set-{uuid.uuid1()}"
    sleep(5)
    no_auth_client.submit_jobs(
        queue=queue_name, job_set_id=job_set_name, job_request_items=submit_sleep_job()
    )
    # Jobs can must be finished before deleting queue
    cancelled_response = no_auth_client.cancel_jobs(
        queue=queue_name, job_set_id=job_set_name
    )
    assert f"all jobs in job set {job_set_name}" == cancelled_response.cancelled_ids[0]


def test_get_job_events_stream():
    job_set_name = f"set-{uuid.uuid1()}"
    jobs = no_auth_client.submit_jobs(
        queue=queue_name, job_set_id=job_set_name, job_request_items=submit_sleep_job()
    )
    sleep(5)
    # Jobs can must be finished before deleting queue
    jobs.job_response_items[0].job_id
    event_stream = no_auth_client.get_job_events_stream(
        queue=queue_name, job_set_id=job_set_name
    )
    # Avoiding fickle tests, we will just pass this test as long as we find an event.
    found_event = False
    for event in event_stream:
        found_event = True
        break
    assert found_event


def submit_sleep_job():
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="sleep",
                image="alpine:latest",
                args=["sleep", "3s"],
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

    return [submit_pb2.JobSubmitRequestItem(priority=0, pod_spec=pod)]
