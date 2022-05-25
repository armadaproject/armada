from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)
from armada_client.armada import (
    submit_pb2,
)


import grpc


tester = ArmadaClient(
    "127.0.0.1", "50051", grpc.insecure_channel(target="127.0.0.1:50051")
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

    tester.submit_jobs(queue="test", job_set_id="test",
                       job_request_items=[submit_pb2.JobSubmitRequestItem(priority=1, pod_spec=pod)])


def test_create_queue():
    tester.create_queue(name="test")


def test_get_queue():
    tester.get_queue("test")


def test_delete_queue():
    tester.delete_queue("test")


def test_get_queue_info():
    tester.get_queue_info(name='test')
