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

no_auth_client = ArmadaClient(
    channel=grpc.insecure_channel(target="127.0.0.1:50051"), max_workers=1
)


@pytest.fixture()
def queue():
    empty_queue = no_auth_client.create_queue(name="test", priority_factor=1)
    yield empty_queue
    no_auth_client.delete_queue(name="test")


def test_submit_job(queue):
    no_auth_client.submit_jobs(
        queue="test", job_set_id="job-set-1", job_request_items=submit_sleep_job()
    )
    # Jobs can must be finished before deleting queue
    no_auth_client.cancel_jobs(queue="test", job_set_id="job-set-1")
    time.sleep(1)


def test_watch_events(queue):
    no_auth_client.submit_jobs(
        queue="test", job_set_id="job-set-1", job_request_items=submit_sleep_job()
    )
    time.sleep(1)
    event_return = no_auth_client.watch_events(queue="test", job_set_id="job-set-1")
    for event in event_return:
        if event.message.succeeded.job_id:
            print(event.message)

    unwatch_events(event_stream=event_return)


def test_get_queue(queue):
    queue = no_auth_client.get_queue(name="test")
    assert queue.name == "test"


def test_get_queue_info(queue):
    queue = no_auth_client.get_queue_info(name="test")
    assert queue.name == "test"
    assert not queue.active_job_sets


def test_delete_queue():
    no_auth_client.delete_queue(name="test")


def submit_sleep_job():
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="sleep",
                image="alpine:latest",
                args=["sleep", "50s"],
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


def test_cancel_by_id(queue):
    jobs = no_auth_client.submit_jobs(
        queue="test", job_set_id="job-set-1", job_request_items=submit_sleep_job()
    )
    no_auth_client.cancel_jobs(job_id=jobs.job_response_items[0].job_id)
    time.sleep(1)


def test_cancel_by_queue_job_set_no_jobs(queue):
    no_auth_client.cancel_jobs(queue="test", job_set_id="job-set-1")
