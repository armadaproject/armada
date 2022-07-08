import os
import uuid

import grpc
from armada_client.client import ArmadaClient


def create_dummy_job():
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

    return [submit_pb2.JobSubmitRequestItem(priority=1, pod_spec=pod)]


def monitor():
    disable_ssl = None
    host = os.getenv("HOST", "localhost")
    port = os.getenv("PORT", "50051")
    queue = "test-monitor"

    if disable_ssl:
        channel_credentials = grpc.local_channel_credentials()
    else:
        channel_credentials = grpc.ssl_channel_credentials()

    channel = grpc.secure_channel(
        f"{host}:{port}",
        channel_credentials,
    )

    client = ArmadaClient(channel)
    try:
        client.create_queue(name=queue, priority_factor=200)
    except grpc.RpcError as e:
        code = e.code()
        if code == grpc.StatusCode.ALREADY_EXISTS:
            print(f"Queue {queue} already exists")
        else:
            raise e

    job_request_items = create_dummy_job(client)
    job_set_id = f"set-{uuid.uuid1()}"

    client.create_job_request(
        queue=queue, job_set_id=job_set_id, job_request_items=job_request_items
    )
    resp = client.submit_jobs(queue, job_set_id, job_request_items)
    job_id = resp.job_response_items[0].job_id

    time.sleep(2)

    event_stream = client.get_job_events_stream(queue=queue, job_set_id=job_set_id)

    for event in event_stream:
        print(event)
        print("=========")
        if job_id == event.message.succeeded.job_id:
            break

    else:
        print("Failed to find successful job")
        exit()

    print(f"Found job: {job_id}")

    client.unwatch_events(event_stream)


def priority():
    disable_ssl = None
    host = os.environ.get("HOST", "localhost")
    port = os.environ.get("PORT", "50051")
    queue = "test-priority"

    if disable_ssl:
        channel_credentials = grpc.local_channel_credentials()
    else:
        channel_credentials = grpc.ssl_channel_credentials()

    channel = grpc.secure_channel(
        f"{host}:{port}",
        channel_credentials,
    )

    client = ArmadaClient(channel)
    try:
        client.create_queue(name=queue, priority_factor=200)
    except grpc.RpcError as e:
        code = e.code()
        if code == grpc.StatusCode.ALREADY_EXISTS:
            print(f"Queue {queue} already exists")
        else:
            raise e

    job_request_items = create_dummy_job(client)
    job_set_id = f"set-{uuid.uuid1()}"

    client.create_job_request(
        queue=queue, job_set_id=job_set_id, job_request_items=job_request_items
    )
    client.submit_jobs(queue, job_set_id, job_request_items)

    client.reprioritize_jobs(new_priority=2, queue=queue, job_set_id=job_set_id)
    client.update_queue(name=queue, priority_factor=2)

    info = client.get_queue(name=queue)
    print(f"Queue {queue} now has a priority factor of {info.priority_factor}")


def queues():
    ssl = os.environ.get("SSL", False)
    host = os.environ.get("HOST", "localhost")
    port = os.environ.get("PORT", "50051")
    queue_name = "test-queues"

    if ssl:
        channel_credentials = grpc.ssl_channel_credentials()

        channel = grpc.secure_channel(
            f"{host}:{port}",
            channel_credentials,
        )
    else:
        channel_credentials = grpc.local_channel_credentials()

        channel = grpc.unsecure_channel(f"{host}:{port}")

    client = ArmadaClient(channel)
    try:
        client.create_queue(name=queue_name, priority_factor=200)
    except grpc.RpcError as e:
        code = e.code()
        if code == grpc.StatusCode.ALREADY_EXISTS:
            print(f"Queue {queue_name} already exists")
        else:
            raise e

    print("============")
    info = client.get_queue_info(name=queue_name)
    print(info)
    print("============")
    info = client.get_queue(name=queue_name)
    print(info)

    client.delete_queue(name=queue_name)


if __name__ == "__main__":
    main()
