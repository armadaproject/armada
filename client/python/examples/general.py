import os
import time
import uuid

import grpc
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)


def create_dummy_job(client):
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


def wait_for_job_event(event_stream, job_id, event_type):
    for event in event_stream:
        if job_id == event.message.succeeded.job_id and event_type == "SUCCEEDED":
            return True

        elif job_id == event.message.running.job_id and event_type == "RUNNING":
            return True

        elif (
            job_id == event.message.reprioritizing.job_id
            and event_type == "REPRIORITIZING"
        ):
            return True

        elif (
            job_id == event.message.reprioritized.job_id
            and event_type == "REPRIORITIZED"
        ):
            return True

        elif job_id == event.message.failed.job_id and event_type == "FAILED":
            return False

        elif job_id == event.message.cancelled.job_id and event_type == "CANCELLED":
            return True

        elif job_id == event.message.cancelling.job_id and event_type == "CANCELLING":
            return True

        elif job_id == event.message.queued.job_id and event_type == "QUEUED":
            return True

    return None


def creating_jobs_example(client, queue, job_set_id):

    job_request_items = create_dummy_job(client)

    client.create_job_request(
        queue=queue, job_set_id=job_set_id, job_request_items=job_request_items
    )

    resp = client.submit_jobs(
        queue=queue, job_set_id=job_set_id, job_request_items=job_request_items
    )
    job_id = resp.job_response_items[0].job_id

    client.reprioritize_jobs(new_priority=2, queue=queue, job_set_id=job_set_id)

    time.sleep(2)

    event_stream = client.get_job_events_stream(queue=queue, job_set_id=job_set_id)

    succeded = wait_for_job_event(event_stream, job_id, "SUCCEEDED")
    if succeded:
        print("Job Succeeded")

    elif not succeded:
        print("Job Failed")

    client.unwatch_events(event_stream)


def creating_queues_example(client, queue):
    try:
        client.create_queue(name=queue, priority_factor=1)
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

    queue = "test-general"
    job_set_id = f"set-{uuid.uuid1()}"

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
