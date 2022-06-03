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

no_auth_client = ArmadaClient(host='127.0.0.1', port=50051,
                              channel=grpc.insecure_channel(target=f"127.0.0.1:50051"))


def test_create_queue():
    no_auth_client.create_queue(name='test', priority_factor=1)
def test_delete_queue():
    no_auth_client.delete_queue(name='test')

def test_submit_job():
    no_auth_client.submit_jobs(queue='test', job_set_id='job-set-1', job_request_items=submit_sleep_job())

def submit_sleep_job():
    pod = core_v1.PodSpec(containers=[
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

    return [submit_pb2.JobSubmitRequestItem(priority=1, pod_spec=pod)]
