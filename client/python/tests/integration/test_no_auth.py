import uuid
from armada_client.armada import (
    submit_pb2,
)
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)
from armada_client.client import unwatch_events
import grpc
import pytest
import time

no_auth_client = ArmadaClient(channel=grpc.insecure_channel(target="127.0.0.1:50051"))


queue_name = f"queue-{uuid.uuid1()}"

@pytest.fixture(scope="session", autouse=True)
def queue():
    no_auth_client.delete_queue(name=queue_name)

    empty_queue = no_auth_client.create_queue(name=queue_name, priority_factor=1)
    yield empty_queue
    no_auth_client.delete_queue(name=queue_name)


def test_submit_job():
    job_set_id = f"set-{uuid.uuid1()}"
    no_auth_client.create_queue(name=queue_name, priority_factor=1)
    no_auth_client.submit_jobs(
        queue=queue_name, job_set_id=job_set_id, job_request_items=submit_sleep_job()
    )
    # Jobs can must be finished before deleting queue
    no_auth_client.cancel_jobs(queue=queue_name, job_set_id=job_set_id)
    time.sleep(1)


def test_watch_events():
    job_set_id = f"set-{uuid.uuid1()}"
    no_auth_client.create_queue(name=queue_name, priority_factor=1)

    sleep_job = no_auth_client.submit_jobs(
        queue=queue_name, job_set_id=job_set_id, job_request_items=submit_sleep_job()
    )
    time.sleep(4)
    sleep_id = sleep_job.job_response_items[0].job_id
    event_return = no_auth_client.watch_events(queue=queue_name, job_set_id=job_set_id)
    found_successful_job = False
    for event in event_return:
        if event.message.succeeded.job_id and event.message.succeeded.job_id == sleep_id:
            found_successful_job = True
            break
    assert found_successful_job


def test_get_queue():
    queue_name = f"queue-{uuid.uuid1()}"

    no_auth_client.create_queue(name=queue_name, priority_factor=1)

    queue = no_auth_client.get_queue(name=queue_name)
    assert queue.name == queue_name


def test_get_queue_info():

    no_auth_client.create_queue(name=queue_name, priority_factor=1)
    queue = no_auth_client.get_queue_info(name=queue_name)
    assert queue.name == queue_name
    assert not queue.active_job_sets


def submit_sleep_job():
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="sleep",
                image="alpine:latest",
                args=["sleep", "2s"],
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


def test_cancel_by_id():
    job_set_id = f"set-{uuid.uuid1()}"
    no_auth_client.create_queue(name=queue_name, priority_factor=1)

    jobs = no_auth_client.submit_jobs(
        queue=queue_name, job_set_id=job_set_id, job_request_items=submit_sleep_job()
    )
    no_auth_client.cancel_jobs(job_id=jobs.job_response_items[0].job_id)
    time.sleep(1)
