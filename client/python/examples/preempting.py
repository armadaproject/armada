"""
A full example of preempting jobs
"""

import os
import uuid

import grpc
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)


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

    return [
        client.create_job_request_item(priority=1, namespace="default", pod_spec=pod)
    ]


def preempting_jobs_example(client, queue):
    """
    Submits and Preempts Job
    """

    # Create the PodSpec for the job
    job_request_items1 = create_dummy_job(client)

    job_set_id1 = f"set-{uuid.uuid1()}"

    resp1 = client.submit_jobs(
        queue=queue, job_set_id=job_set_id1, job_request_items=job_request_items1
    )

    # Gets the job_id of the first job in job_request_items1
    # This job is preempted using the job_id
    job_id = resp1.job_response_items[0].job_id
    print(f"preempting {queue} {job_set_id1} {job_id}")
    client.preempt_jobs(queue=queue, job_set_id=job_set_id1, job_id=job_id)


def quick_create_queue(client, queue):
    """
    Creates a queue.

    Will skip if the queue already exists.
    """

    queue_req = client.create_queue_request(name=queue, priority_factor=1)

    # Make sure we handle the queue already existing
    try:
        client.create_queue(queue_req)

    # Handle the error we expect to maybe occur
    except grpc.RpcError as e:
        code = e.code()
        if code == grpc.StatusCode.ALREADY_EXISTS:
            print(f"Queue {queue} already exists")
            client.update_queue(queue_req)
        else:
            raise e


def workflow():
    """
    Starts a workflow, which includes:
        - Creating a queue and job
        - Preempting the job with its job-id, queue and jobset-id
    """

    # The queue and job_set_id that will be used for all jobs
    queue = "test-preempting"

    # Ensures that the correct channel type is generated
    if DISABLE_SSL:
        channel = grpc.insecure_channel(f"{HOST}:{PORT}")
    else:
        channel_credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(
            f"{HOST}:{PORT}",
            channel_credentials,
        )

    client = ArmadaClient(channel)
    quick_create_queue(client, queue)

    preempting_jobs_example(client, queue)


if __name__ == "__main__":
    # Note that the form of ARMADA_SERVER should be something like
    # domain.com, localhost, or 0.0.0.0
    DISABLE_SSL = os.environ.get("DISABLE_SSL", False)
    HOST = os.environ.get("ARMADA_SERVER", "localhost")
    PORT = os.environ.get("ARMADA_PORT", "50051")

    workflow()
    print("Completed Workflow")
