import uuid
from armada_client.armada import (
    submit_pb2,
)
from armada_client.client import ArmadaClient, search_for_job_complete
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)
from armada_client.client import unwatch_events
import grpc
import time

no_auth_client = ArmadaClient(channel=grpc.insecure_channel(target="127.0.0.1:50051"))


def test_submit_job():
    queue_name = f"queue-{uuid.uuid1()}"
    job_set_name = f"set-{uuid.uuid1()}"
    no_auth_client.create_queue(name=queue_name, priority_factor=200)
    no_auth_client.submit_jobs(
        queue=queue_name, job_set_id=job_set_name, job_request_items=submit_sleep_job()
    )
    # Jobs can must be finished before deleting queue
    no_auth_client.cancel_jobs(queue=queue_name, job_set_id=job_set_name)
    no_auth_client.delete_queue(name=queue_name)
    time.sleep(1)


def test_submit_job_and_cancel_by_id():
    queue_name = f"queue-{uuid.uuid1()}"
    job_set_name = f"set-{uuid.uuid1()}"
    no_auth_client.create_queue(name=queue_name, priority_factor=200)
    jobs = no_auth_client.submit_jobs(
        queue=queue_name, job_set_id=job_set_name, job_request_items=submit_sleep_job()
    )
    # Jobs can must be finished before deleting queue
    no_auth_client.cancel_jobs(job_id=jobs.job_response_items[0].job_id)
    no_auth_client.delete_queue(name=queue_name)


def test_submit_job_and_cancel_by_queue_job_id():
    queue_name = f"queue-{uuid.uuid1()}"
    job_set_name = f"set-{uuid.uuid1()}"
    no_auth_client.create_queue(name=queue_name, priority_factor=200)
    no_auth_client.submit_jobs(
        queue=queue_name, job_set_id=job_set_name, job_request_items=submit_sleep_job()
    )
    # Jobs can must be finished before deleting queue
    no_auth_client.cancel_jobs(queue=queue_name, job_set_id=job_set_name)
    no_auth_client.delete_queue(name=queue_name)


def test_get_queue():
    queue_name = f"queue-{uuid.uuid1()}"
    no_auth_client.create_queue(name=queue_name, priority_factor=200)

    queue = no_auth_client.get_queue(name=queue_name)
    assert queue.name == queue_name
    no_auth_client.delete_queue(name=queue_name)


def test_get_queue_info():
    queue_name = f"queue-{uuid.uuid1()}"
    no_auth_client.create_queue(name=queue_name, priority_factor=200)

    queue = no_auth_client.get_queue_info(name=queue_name)
    assert queue.name == queue_name
    assert not queue.active_job_sets
    no_auth_client.delete_queue(name=queue_name)


def test_get_job_events_stream():
    queue_name = f"queue-{uuid.uuid1()}"
    job_set_name = f"set-{uuid.uuid1()}"
    no_auth_client.delete_queue(name=queue_name)
    no_auth_client.create_queue(name=queue_name, priority_factor=1.0)
    jobs = no_auth_client.submit_jobs(
        queue=queue_name, job_set_id=job_set_name, job_request_items=submit_sleep_job()
    )
    time.sleep(2)
    # Jobs can must be finished before deleting queue
    job_id = jobs.job_response_items[0].job_id
    event_stream = no_auth_client.get_job_events_stream(
        queue=queue_name, job_set_id=job_set_name
    )
    success = search_for_job_complete(event_stream, job_name="sleep", job_id=job_id)
    assert success[0] == "successful"
    assert success[1] == f"Armada job:id sleep:{job_id} succeeded"

def test_get_job_events_stream_error():
    queue_name = f"queue-{uuid.uuid1()}"
    job_set_name = f"set-{uuid.uuid1()}"
    no_auth_client.delete_queue(name=queue_name)
    no_auth_client.create_queue(name=queue_name, priority_factor=1.0)
    jobs = no_auth_client.submit_jobs(
        queue=queue_name, job_set_id=job_set_name, job_request_items=submit_bad_job()
    )
    time.sleep(2)
    # Jobs can must be finished before deleting queue
    job_id = jobs.job_response_items[0].job_id
    event_stream = no_auth_client.get_job_events_stream(
        queue=queue_name, job_set_id=job_set_name
    )
    found_error_job = False
    for event in event_stream:
        print(event.message.failed)
        if job_id == event.message.failed.job_id:
            found_error_job = True
            break
    assert found_error_job


def submit_sleep_job():
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="sleep",
                image="alpine:latest",
                args=["sleep", "3s"],
                resources=core_v1.ResourceRequirements(
                    requests={
                        "cpu": api_resource.Quantity(string="0.2"),
                        "memory": api_resource.Quantity(string="64Mi"),
                    },
                    limits={
                        "cpu": api_resource.Quantity(string="0.2"),
                        "memory": api_resource.Quantity(string="64Mi"),
                    },
                ),
            )
        ],
    )

    return [submit_pb2.JobSubmitRequestItem(priority=0, pod_spec=pod)]


def submit_bad_job():
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="no_image",
                image="alpine:latest",
                args=["sleep", "3s"],
                resources=core_v1.ResourceRequirements(
                    requests={
                        "cpu": api_resource.Quantity(string="0.2"),
                        "memory": api_resource.Quantity(string="64Mi"),
                    },
                    limits={
                        "cpu": api_resource.Quantity(string="0.2"),
                        "memory": api_resource.Quantity(string="64Mi"),
                    },
                ),
            )
        ],
    )

    return [submit_pb2.JobSubmitRequestItem(priority=0, pod_spec=pod)]
