"""
Example of using the Armada client to create a queue, jobset and job,
then watch for the job to succeed or fail - that also uses basic auth!
"""

import base64
import os
import uuid

import grpc
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)


class GrpcBasicAuth(grpc.AuthMetadataPlugin):
    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password
        super().__init__()

    def __call__(self, context, callback):
        b64encoded_auth = base64.b64encode(
            bytes(f"{self._username}:{self._password}", "utf-8")
        ).decode("ascii")
        callback((("authorization", f"basic {b64encoded_auth}"),), None)


def create_dummy_job(client: ArmadaClient):
    """
    Create a dummy job with a single container.
    """

    # For infomation on where this comes from,
    # see https://github.com/kubernetes/api/blob/master/core/v1/generated.proto
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="container1",
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

    return [client.create_job_request_item(priority=1, pod_spec=pod)]


def simple_workflow():
    """
    Starts a workflow, which includes:
        - Creating a queue
        - Creating a jobset
        - Creating a job
    """

    # The queue and job_set_id that will be used for all jobs
    queue = f"simple-queue-{uuid.uuid1()}"
    job_set_id = f"simple-jobset-{uuid.uuid1()}"

    channel_credentials = grpc.local_channel_credentials()

    channel_creds = grpc.composite_channel_credentials(
        channel_credentials,
        grpc.metadata_call_credentials(
            GrpcBasicAuth(username="armada-user", password="armada-pass")
        ),
    )

    channel = grpc.secure_channel(f"{HOST}:{PORT}", channel_creds)

    client = ArmadaClient(channel)

    queue_req = client.create_queue_request(name=queue, priority_factor=1)
    client.create_queue(queue_req)

    # Create the PodSpec for the job
    job_request_items = create_dummy_job(client)

    client.submit_jobs(
        queue=queue, job_set_id=job_set_id, job_request_items=job_request_items
    )


if __name__ == "__main__":
    # Note that the form of ARMADA_SERVER should be something like
    # domain.com, localhost, or 0.0.0.0
    HOST = os.environ.get("ARMADA_SERVER", "localhost")
    PORT = os.environ.get("ARMADA_PORT", "50051")

    simple_workflow()
    print("Completed Workflow")
