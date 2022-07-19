"""
Example of using the armada client to create a queue, jobset and job,
then watch for the job to succeed or fail.
"""

import os
import time
import uuid

import grpc
from armada_client.client import ArmadaClient
from armada_client.event import EventType

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

    return [client.create_job_request_item(priority=1, pod_spec=pod)]


def wait_for_job_event(client, event_stream, job_id: str, event_state: str):
    """
    Wait for a job event to occur.
    Will automatically return if the job fails or the event stream is closed.
    """

    # Contains all the possible message types
    for event in event_stream:

        event = client.unmarshal_event_response(event)

        if event.message.job_id == job_id:
            if event.type == EventType.failed:
                return False
            elif event.type == event_state:
                return True


def creating_jobs_example(client, queue, job_set_id):
    """
    Creates a jobset and job, and makes sure the job completes successfully.
    """

    # Create the PodSpec for the job
    job_request_items = create_dummy_job(client)

    client.create_job_request(
        queue=queue, job_set_id=job_set_id, job_request_items=job_request_items
    )
    resp = client.submit_jobs(
        queue=queue, job_set_id=job_set_id, job_request_items=job_request_items
    )

    # Gets the job_id of the first job in the job_request_items
    job_id = resp.job_response_items[0].job_id

    client.reprioritize_jobs(new_priority=2, queue=queue, job_set_id=job_set_id)

    # Needed to allow for the delay in the job_set being created
    time.sleep(2)

    # Event stream for the job_set
    # Can be accessed directly as an iterator that will yield the next event
    event_stream = client.get_job_events_stream(queue=queue, job_set_id=job_set_id)

    test = wait_for_job_event(client, event_stream, job_id, EventType.succeeded)
    if test:
        print("Job submitted")

    elif not test:
        print("Failed")

    # Close the event stream
    client.unwatch_events(event_stream)


def creating_queues_example(client, queue):
    """
    Creates a queue.
    Will skip if the queue already exists.
    Also changes the priority of the queue to 2 for demonstration purposes.
    """

    # Make sure we handle the queue already existing
    try:
        client.create_queue(name=queue, priority_factor=1)

    # Handle the error we expect to maybe occur
    except grpc.RpcError as e:
        code = e.code()
        if code == grpc.StatusCode.ALREADY_EXISTS:
            print(f"Queue {queue} already exists")
            client.update_queue(name=queue, priority_factor=1)
        else:
            raise e

    info = client.get_queue(name=queue)
    print(f"Queue {queue} currently has a priority factor of {info.priority_factor}")

    info = client.get_queue(name=queue)

    client.update_queue(name=queue, priority_factor=2)

    info = client.get_queue(name=queue)
    print(f"Queue {queue} now has a priority factor of {info.priority_factor}")


def workflow():
    """
    Starts a workflow, which includes:
        - Creating a queue
        - Creating a jobset
        - Creating a job
    """

    # The queue and job_set_id that will be used for all jobs
    queue = "test-general"
    job_set_id = f"set-{uuid.uuid1()}"

    # Ensures that the correct channel type is generated
    if DISABLE_SSL:
        channel_credentials = grpc.local_channel_credentials()
    else:
        channel_credentials = grpc.ssl_channel_credentials()
    channel = grpc.secure_channel(
        f"{HOST}:{PORT}",
        channel_credentials,
    )

    client = ArmadaClient(channel)

    creating_queues_example(client, queue)
    creating_jobs_example(client, queue, job_set_id)


if __name__ == "__main__":
    DISABLE_SSL = os.environ.get("True", False)
    HOST = os.environ.get("HOST", "localhost")
    PORT = os.environ.get("PORT", "50051")

    workflow()
    print("Completed Workflow")
