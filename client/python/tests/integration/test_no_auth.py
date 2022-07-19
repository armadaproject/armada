import uuid
import time
import os
import grpc

import pytest


from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)


def client() -> ArmadaClient:
    server_name = os.environ.get("ARMADA_SERVER", "localhost")
    server_port = os.environ.get("ARMADA_PORT", "50051")
    server_ssl = os.environ.get("ARMADA_SSL", "false")

    if server_ssl == "true":
        channel_credentials = grpc.ssl_channel_credentials()
        return ArmadaClient(
            channel=grpc.secure_channel(
                f"{server_name}:{server_port}", channel_credentials
            )
        )

    else:
        return ArmadaClient(
            channel=grpc.insecure_channel(f"{server_name}:{server_port}")
        )


no_auth_client = client()

queue_name = f"queue-{uuid.uuid1()}"


def submit_sleep_job():
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="sleep",
                image="alpine:latest",
                args=["sleep", "30s"],
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

    return [no_auth_client.create_job_request_item(priority=0, pod_spec=pod)]


def wait_for(queue=None, job_set_id=None):

    timeout = 20

    while True:
        try:
            if queue:
                no_auth_client.get_queue(name=queue)
                return True

            if job_set_id:
                events = no_auth_client.get_job_events_stream(
                    queue=queue_name, job_set_id=job_set_id
                )
                for _ in events:
                    break

                return True

        except grpc.RpcError as e:
            code = e.code()
            if code != grpc.StatusCode.NOT_FOUND:
                raise e

        timeout -= 1

        time.sleep(1)

        if timeout <= 0:
            raise Exception("Timeout")


@pytest.fixture(scope="session", autouse=True)
def create_queue():

    no_auth_client.create_queue(name=queue_name, priority_factor=1)
    wait_for(queue=queue_name)


def test_get_queue():
    queue = no_auth_client.get_queue(name=queue_name)
    assert queue.name == queue_name


def test_get_queue_info():
    queue = no_auth_client.get_queue_info(name=queue_name)
    assert queue.name == queue_name
    assert not queue.active_job_sets


def test_submit_job_and_cancel_by_id():
    job_set_name = f"set-{uuid.uuid1()}"
    jobs = no_auth_client.submit_jobs(
        queue=queue_name, job_set_id=job_set_name, job_request_items=submit_sleep_job()
    )

    wait_for(job_set_id=job_set_name)

    cancelled_message = no_auth_client.cancel_jobs(
        job_id=jobs.job_response_items[0].job_id
    )

    assert cancelled_message.cancelled_ids[0] == jobs.job_response_items[0].job_id


def test_submit_job_and_cancel_by_queue_job_set():
    job_set_name = f"set-{uuid.uuid1()}"
    no_auth_client.submit_jobs(
        queue=queue_name, job_set_id=job_set_name, job_request_items=submit_sleep_job()
    )

    wait_for(job_set_id=job_set_name)

    cancelled_message = no_auth_client.cancel_jobs(
        queue=queue_name, job_set_id=job_set_name
    )

    expected = f"all jobs in job set {job_set_name}"
    assert expected == cancelled_message.cancelled_ids[0]


def test_get_job_events_stream():
    job_set_name = f"set-{uuid.uuid1()}"
    no_auth_client.submit_jobs(
        queue=queue_name, job_set_id=job_set_name, job_request_items=submit_sleep_job()
    )

    wait_for(job_set_id=job_set_name)

    event_stream = no_auth_client.get_job_events_stream(
        queue=queue_name, job_set_id=job_set_name
    )

    # Avoiding fickle tests, we will just pass this test as long as we find an event.
    found_event = False
    for _ in event_stream:
        found_event = True
        break
    assert found_event
