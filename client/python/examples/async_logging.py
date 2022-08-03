"""
Demonstrates how to run jobs, and also log all
changes to that job or job_set in realtime concurrently.
"""

import os
import threading
import time
import uuid

import grpc
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)


def create_dummy_job(client):
    """
    Create a dummy job with a single container.
    """

    # For infomation on where this comes from,
    # see: https://github.com/kubernetes/api/blob/master/core/v1/generated.proto
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


def watch_job_set(client: ArmadaClient, queue: str, job_set_id):
    """
    Trys to latch on to the job set and print out the status

    This is a blocking call, so it will never return.

    If there have been more than 10 failed connections, then it will exit
    """
    attempts = 0

    # Continuely try and reconnected to the job set
    while True:
        try:
            event_stream = client.get_job_events_stream(
                queue=queue, job_set_id=job_set_id
            )

            # For each event, check if it is one we are interested in
            # and print out the message if it is
            for event in event_stream:

                event = client.unmarshal_event_response(event)

                print(f"Job {event.message.job_id} - {event.type}")

        # Handle the error we expect to maybe occur
        except grpc.RpcError as e:
            if e.code() != grpc.StatusCode.NOT_FOUND:
                print("Unexpected error:", e)
                exit()

        attempts += 1
        time.sleep(1)

        if attempts > 10:
            print("Exiting - too many attempts")
            exit()


def run_workflow(client, queue, job_set_id):
    """
    Example workflow for the async logging example
    """

    # Handle if the queue already exists
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

    # Time for the watcher to pick it up
    time.sleep(1)

    # Some different commands for logging to detect
    client.update_queue(name=queue, priority_factor=2)

    job_request_items = create_dummy_job(client)

    client.submit_jobs(
        queue=queue, job_set_id=job_set_id, job_request_items=job_request_items
    )
    client.reprioritize_jobs(new_priority=2, queue=queue, job_set_id=job_set_id)


def main():
    """
    Run the example workflow, and both the watchers in separate threads
    """

    # The queue and job_set_id that will be used for all jobs
    queue = "test-general"
    job_set_id = f"set-{uuid.uuid1()}"

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

    # Create the threads
    workflow = threading.Thread(target=run_workflow, args=(client, queue, job_set_id))

    watch_jobs = threading.Thread(
        target=watch_job_set, args=(client, queue, job_set_id)
    )

    # Start the threads
    workflow.start()
    watch_jobs.start()

    # wait for threads to finish
    workflow.join()
    watch_jobs.join()

    print("Completed.")


if __name__ == "__main__":
    # Note that the form of ARMADA_SERVER should be something like
    # domain.com, localhost, or 0.0.0.0
    DISABLE_SSL = os.environ.get("DISABLE_SSL", False)
    HOST = os.environ.get("ARMADA_SERVER", "localhost")
    PORT = os.environ.get("ARMADA_PORT", "50051")

    main()
