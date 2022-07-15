import uuid
from armada_client.armada import (
    submit_pb2,
)
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)
import grpc

from armada.operators.jobservice import JobServiceClient
from armada.operators.utils import search_for_job_complete

no_auth_client = ArmadaClient(channel=grpc.insecure_channel(target="127.0.0.1:50051"))
job_service_client = JobServiceClient(
    channel=grpc.insecure_channel(target="127.0.0.1:60003")
)


def sleep_job():
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="goodsleep",
                image="busybox",
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
    return [submit_pb2.JobSubmitRequestItem(priority=0, pod_spec=pod)]


def bad_sleep_job():
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="badsleep",
                image="NOTACONTAINER",
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
    return [submit_pb2.JobSubmitRequestItem(priority=0, pod_spec=pod)]


def test_success_job():
    job_set_name = f"set-{uuid.uuid1()}"

    job = no_auth_client.submit_jobs(
        queue="test",
        job_set_id=job_set_name,
        job_request_items=sleep_job(),
    )
    job_id = job.job_response_items[0].job_id

    job_state, job_message = search_for_job_complete(
        job_service_client=job_service_client,
        queue="test",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=job_id,
    )
    assert job_state == "succeeded"
    assert job_message == f"Armada test:{job_id} succeeded"


def test_bad_job():
    job_set_name = "test-bad-job"

    job = no_auth_client.submit_jobs(
        queue="test",
        job_set_id=job_set_name,
        job_request_items=bad_sleep_job(),
    )
    job_id = job.job_response_items[0].job_id

    job_state, job_message = search_for_job_complete(
        job_service_client=job_service_client,
        queue="test",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=job_id,
    )
    assert job_state == "failed"
    assert job_message.startswith(f"Armada test:{job_id} failed")

def test_two_jobs():
    job_set_name = f"test-{uuid.uuid1()}"

    first_job = no_auth_client.submit_jobs(
        queue="test",
        job_set_id=job_set_name,
        job_request_items=sleep_job(),
    )
    first_job_id = first_job.job_response_items[0].job_id

    job_state, job_message = search_for_job_complete(
        job_service_client=job_service_client,
        queue="test",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=first_job_id,
    )
    assert job_state == "succeeded"
    assert job_message == f"Armada test:{first_job_id} succeeded"

    second_job = no_auth_client.submit_jobs(
        queue="test",
        job_set_id=job_set_name,
        job_request_items=sleep_job(),
    )
    second_job_id = second_job.job_response_items[0].job_id


    job_state, job_message = search_for_job_complete(
        job_service_client=job_service_client,
        queue="test",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=second_job_id,
    )
    assert job_state == "succeeded"
    assert job_message == f"Armada test:{second_job_id} succeeded"
