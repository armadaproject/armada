import os
import time
import uuid

import grpc
import pytest
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)
from armada_client.typings import JobState


def submit_sleep_job(client):
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

    return [client.create_job_request_item(priority=0, pod_spec=pod)]


def wait_for(client: ArmadaClient, queue, job_set_id=None):
    """
    Waits for a queue and optionally the job_set_id to be active.

    Ensures that following steps will not fail.
    """

    timeout = 20

    while True:
        try:

            # queue active test
            client.get_queue(name=queue)

            if job_set_id:
                events = client.get_job_events_stream(
                    queue=queue, job_set_id=job_set_id
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


@pytest.fixture(scope="session", name="client")
def no_auth_client() -> ArmadaClient:
    server_name = os.environ.get("ARMADA_SERVER", "localhost")
    server_port = os.environ.get("ARMADA_PORT", "50051")
    server_ssl = os.environ.get("ARMADA_SSL", "false")

    if server_ssl.lower() == "true":
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


@pytest.fixture(scope="session", name="queue_name")
def get_queue():
    return f"queue-{uuid.uuid1()}"


@pytest.fixture(scope="session", autouse=True)
def create_queue(client: ArmadaClient, queue_name):

    queue = client.create_queue_request(name=queue_name, priority_factor=1)
    client.create_queue(queue)
    wait_for(client, queue=queue_name)


def test_batch_update_and_create_queues(client: ArmadaClient):
    # Need to separately create queue name so that it is not
    # automatically created by the fixture.
    queue_name1 = f"queue-{uuid.uuid1()}"
    queue_name2 = f"queue-{uuid.uuid1()}"

    queue1 = client.create_queue_request(name=queue_name1, priority_factor=1)
    queue2 = client.create_queue_request(name=queue_name2, priority_factor=1)
    client.create_queues([queue1, queue2])

    queue1 = client.get_queue(name=queue_name1)
    queue2 = client.get_queue(name=queue_name2)

    assert queue1.priority_factor == queue2.priority_factor

    updated_queue1 = client.create_queue_request(name=queue_name1, priority_factor=2)
    updated_queue2 = client.create_queue_request(name=queue_name2, priority_factor=2)
    client.update_queues([updated_queue1, updated_queue2])

    queue1 = client.get_queue(name=queue_name1)
    queue2 = client.get_queue(name=queue_name2)

    assert queue1.priority_factor == queue2.priority_factor


def test_get_queue(client: ArmadaClient, queue_name):
    queue = client.get_queue(name=queue_name)
    assert queue.name == queue_name


def test_get_queue_info(client: ArmadaClient, queue_name):
    queue = client.get_queue_info(name=queue_name)
    assert queue.name == queue_name
    assert not queue.active_job_sets


def test_submit_job_and_cancel_by_id(client: ArmadaClient, queue_name):
    job_set_name = f"set-{uuid.uuid1()}"
    jobs = client.submit_jobs(
        queue=queue_name,
        job_set_id=job_set_name,
        job_request_items=submit_sleep_job(client),
    )

    wait_for(client, queue=queue_name, job_set_id=job_set_name)

    cancelled_message = client.cancel_jobs(job_id=jobs.job_response_items[0].job_id)

    assert cancelled_message.cancelled_ids[0] == jobs.job_response_items[0].job_id


def test_submit_job_and_cancel_by_queue_job_set(client: ArmadaClient, queue_name):
    job_set_name = f"set-{uuid.uuid1()}"
    client.submit_jobs(
        queue=queue_name,
        job_set_id=job_set_name,
        job_request_items=submit_sleep_job(client),
    )

    wait_for(client, queue=queue_name, job_set_id=job_set_name)

    cancelled_message = client.cancel_jobs(queue=queue_name, job_set_id=job_set_name)

    expected = f"all jobs in job set {job_set_name}"
    assert expected == cancelled_message.cancelled_ids[0]


def test_submit_job_and_cancel_by_job_id(client: ArmadaClient, queue_name):
    job_set_name = f"set-{uuid.uuid1()}"
    jobs = client.submit_jobs(
        queue=queue_name,
        job_set_id=job_set_name,
        job_request_items=submit_sleep_job(client),
    )

    job_id = jobs.job_response_items[0].job_id

    wait_for(client, queue=queue_name, job_set_id=job_set_name)

    cancelled_message = client.cancel_jobs(job_id=job_id)

    assert cancelled_message.cancelled_ids[0] == job_id


def test_submit_job_and_cancelling_with_filter(client: ArmadaClient, queue_name):
    job_set_name = f"set-{uuid.uuid4()}"
    client.submit_jobs(
        queue=queue_name,
        job_set_id=job_set_name,
        job_request_items=submit_sleep_job(client),
    )

    wait_for(client, queue=queue_name, job_set_id=job_set_name)

    client.cancel_jobset(
        queue=queue_name,
        job_set_id=job_set_name,
        filter_states=[JobState.RUNNING, JobState.PENDING],
    )


def test_get_job_events_stream(client: ArmadaClient, queue_name):
    job_set_name = f"set-{uuid.uuid1()}"
    client.submit_jobs(
        queue=queue_name,
        job_set_id=job_set_name,
        job_request_items=submit_sleep_job(client),
    )

    wait_for(client, queue=queue_name, job_set_id=job_set_name)

    event_stream = client.get_job_events_stream(
        queue=queue_name, job_set_id=job_set_name
    )

    # Avoiding fickle tests, we will just pass this test as long as we find an event.
    found_event = False
    for _ in event_stream:
        found_event = True
        break
    assert found_event
