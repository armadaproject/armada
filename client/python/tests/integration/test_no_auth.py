from armada_client.armada import (
    event_pb2,
    event_pb2_grpc,
    queue_pb2,
    queue_pb2_grpc,
    usage_pb2,
    usage_pb2_grpc,
    submit_pb2,
    submit_pb2_grpc,
)
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)
import grpc
import pytest

no_auth_client = ArmadaClient(channel=grpc.insecure_channel(target=f"127.0.0.1:50051"))


@pytest.fixture()
def queue():
    empty_queue = no_auth_client.create_queue(name="test", priority_factor=1)
    yield empty_queue
    no_auth_client.delete_queue(name="test")


def test_submit_job(queue):
    no_auth_client.submit_jobs(
        queue="test", job_set_id="job-set-1", job_request_items=submit_sleep_job()
    )


def test_get_queue(queue):
    queue = no_auth_client.get_queue(name="test")
    assert queue.name == "test"


def test_get_queue_fail():
    queue = no_auth_client.get_queue(name="test")
    assert queue.name == "test"


def submit_sleep_job():
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="sleep",
                image="alpine:latest",
                args=["sleep", "10s"],
                resources=core_v1.ResourceRequirements(
                    requests={
                        "cpu": api_resource.Quantity(string="150m"),
                        "memory": api_resource.Quantity(string="64Mi"),
                    },
                    limits={
                        "cpu": api_resource.Quantity(string="150m"),
                        "memory": api_resource.Quantity(string="64Mi"),
                    },
                ),
            )
        ],
    )

    return [submit_pb2.JobSubmitRequestItem(priority=0, pod_spec=pod)]
