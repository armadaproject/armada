from concurrent import futures

import grpc
import pytest

from server_mock import EventService, SubmitService

from armada_client.armada import event_pb2_grpc, submit_pb2_grpc
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)


@pytest.fixture(scope="session", autouse=True)
def server_mock():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    submit_pb2_grpc.add_SubmitServicer_to_server(SubmitService(), server)
    event_pb2_grpc.add_EventServicer_to_server(EventService(), server)
    server.add_insecure_port("[::]:50051")
    server.start()

    yield
    server.stop(False)


channel = grpc.insecure_channel(target="127.0.0.1:50051")
tester = ArmadaClient(
    grpc.insecure_channel(
        target="127.0.0.1:50051",
        options={
            "grpc.keepalive_time_ms": 30000,
        }.items(),
    )
)


def test_submit_job():
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="Container1",
                image="index.docker.io/library/ubuntu:latest",
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

    tester.submit_jobs(
        queue="test",
        job_set_id="test",
        job_request_items=[tester.create_job_request_item(priority=1, pod_spec=pod)],
    )


def test_create_queue():
    tester.create_queue(name="test", priority_factor=1)


def test_get_queue():
    assert tester.get_queue("test").name == "test"


def test_delete_queue():
    tester.delete_queue("test")


def test_get_queue_info():
    tester.get_queue_info(name="test")


def test_cancel_jobs():
    test_create_queue()
    test_submit_job()
    tester.cancel_jobs(queue="test", job_set_id="job-set-1", job_id="job1")


def test_update_queue():
    tester.update_queue(name="test", priority_factor=1)


def test_reprioritize_jobs():
    tester.reprioritize_jobs(
        new_priority=1.0, job_ids="test", job_set_id="job_test_1", queue="test"
    )
