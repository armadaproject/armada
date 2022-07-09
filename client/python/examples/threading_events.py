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


def useful_message(message, queue):

    acceptable = [
        (message.running, "Running"),
        (message.succeeded, "Succeeded"),
        (message.failed, "Failed"),
        (message.reprioritizing, "Reprioritizing"),
        (message.failed, "Failed"),
        (message.cancelled, "Cancelled"),
        (message.cancelling, "Cancelling"),
        (message.reprioritized, "Reprioritized"),
        (message.reprioritizing, "Reprioritizing"),
        (message.queued, "Queued"),
    ]

    for accepted, msg_type in acceptable:
        if accepted.queue == queue:
            return accepted, msg_type, True

    return None, None, False


def watch_job_set(client: ArmadaClient, queue: str, job_set_id):
    attempts = 0

    while True:
        try:
            event_stream = client.get_job_events_stream(
                queue=queue, job_set_id=job_set_id
            )
            for event in event_stream:
                msg, msg_type, useful = useful_message(event.message, queue)
                if useful:
                    print(msg_type, ":", msg.job_id)

        except grpc.RpcError as e:
            # not found
            if e.code() != grpc.StatusCode.NOT_FOUND:
                print("Unexpected error:", e)
                exit()

        attempts += 1
        time.sleep(1)

        if attempts > 10:
            print("Exiting - too many attempts")
            exit()


def watch_queue(client, queue):
    last_info1 = None
    last_info2 = None

    while True:
        info1 = client.get_queue_info(name=queue)
        info2 = client.get_queue(name=queue)

        if last_info1 is None:
            last_info1 = info1
            last_info2 = info2

        if last_info2.priority_factor != info2.priority_factor:
            print(
                f"Priority factor changed from {last_info2.priority_factor} to {info2.priority_factor}"
            )

        for new, old in zip(info1.active_job_sets, last_info1.active_job_sets):
            if new.leased_jobs != old.leased_jobs:
                print(
                    f"Leased jobs changed from {old.leased_jobs} to {new.leased_jobs}"
                )

        last_info1 = info1
        last_info2 = info2

        time.sleep(0.2)


def sync_tasks(client, queue, job_set_id):
    try:
        client.create_queue(name=queue, priority_factor=1)
    except grpc.RpcError as e:
        code = e.code()
        if code == grpc.StatusCode.ALREADY_EXISTS:
            print(f"Queue {queue} already exists")
            client.update_queue(name=queue, priority_factor=1)
        else:
            raise e

    # Time for the watcher to pick it up
    time.sleep(1)

    client.update_queue(name=queue, priority_factor=2)

    job_request_items = create_dummy_job(client)

    client.create_job_request(
        queue=queue, job_set_id=job_set_id, job_request_items=job_request_items
    )

    client.submit_jobs(queue, job_set_id, job_request_items)
    client.reprioritize_jobs(new_priority=2, queue=queue, job_set_id=job_set_id)


def main():
    disable_ssl = None
    host = os.environ.get("HOST", "localhost")
    port = os.environ.get("PORT", "50051")
    queue = "test-general"
    job_set_id = f"set-{uuid.uuid1()}"

    if disable_ssl:
        channel_credentials = grpc.local_channel_credentials()
    else:
        channel_credentials = grpc.ssl_channel_credentials()

    channel = grpc.secure_channel(
        f"{host}:{port}",
        channel_credentials,
    )

    client = ArmadaClient(channel)

    # run creating_queues_example in a thread
    thread = threading.Thread(target=sync_tasks, args=(client, queue, job_set_id))
    thread.start()

    # run watch_jobs in a separate thread
    watch_jobs = threading.Thread(
        target=watch_job_set, args=(client, queue, job_set_id)
    )
    watch_jobs.start()

    # run watch in a separate thread
    watch_queues = threading.Thread(target=watch_queue, args=(client, queue))
    watch_queues.start()

    # wait for threads to finish
    thread.join()

    print("Done")


if __name__ == "__main__":
    main()
