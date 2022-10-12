import base64
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
from armada.operators.utils import JobState, search_for_job_complete

job_service_client = JobServiceClient(
    channel=grpc.insecure_channel(target="127.0.0.1:60003")
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

class BasicAuthTest:
    def __init__(self, host, port, username, password, disable_ssl=False):
        if disable_ssl:
            channel_credentials = grpc.local_channel_credentials()
        else:
            channel_credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(
            f"{host}:{port}",
            grpc.composite_channel_credentials(
                channel_credentials,
                grpc.metadata_call_credentials(GrpcBasicAuth(username, password)),
            ),
        )
        self.client = ArmadaClient(channel)

no_auth_client = BasicAuthTest(host="localhost", port=50051, username="user1", password="password1", disable_ssl=True).client
def sleep_pod(image: str):
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="goodsleep",
                image=image,
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

def test_create_queue():
    no_auth_client.create_queue(submit_pb2.Queue(name="test", priority_factor=1.0))

def test_delete_queue():
    no_auth_client.delete_queue(name="test")

def test_success_job():
    job_set_name = f"test-{uuid.uuid1()}"
    job = no_auth_client.submit_jobs(
        queue="test",
        job_set_id=job_set_name,
        job_request_items=sleep_pod(image="busybox"),
    )
    job_id = job.job_response_items[0].job_id

    job_state, job_message = search_for_job_complete(
        job_service_client=job_service_client,
        armada_queue="test",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=job_id,
    )
    assert job_state == JobState.SUCCEEDED
    assert job_message == f"Armada test:{job_id} succeeded"


def test_bad_job():
    job_set_name = f"test-{uuid.uuid1()}"

    job = no_auth_client.submit_jobs(
        queue="test",
        job_set_id=job_set_name,
        job_request_items=sleep_pod(image="NOTACONTAINER"),
    )
    job_id = job.job_response_items[0].job_id

    job_state, job_message = search_for_job_complete(
        job_service_client=job_service_client,
        armada_queue="test",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=job_id,
    )
    assert job_state == JobState.FAILED
    assert job_message.startswith(f"Armada test:{job_id} failed")


def test_two_jobs():
    job_set_name = f"test-{uuid.uuid1()}"

    first_job = no_auth_client.submit_jobs(
        queue="test",
        job_set_id=job_set_name,
        job_request_items=sleep_pod(image="busybox"),
    )
    first_job_id = first_job.job_response_items[0].job_id

    job_state, job_message = search_for_job_complete(
        job_service_client=job_service_client,
        armada_queue="test",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=first_job_id,
    )
    assert job_state == JobState.SUCCEEDED
    assert job_message == f"Armada test:{first_job_id} succeeded"

    second_job = no_auth_client.submit_jobs(
        queue="test",
        job_set_id=job_set_name,
        job_request_items=sleep_pod(image="busybox"),
    )
    second_job_id = second_job.job_response_items[0].job_id

    job_state, job_message = search_for_job_complete(
        job_service_client=job_service_client,
        armada_queue="test",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=second_job_id,
    )
    assert job_state == JobState.SUCCEEDED
    assert job_message == f"Armada test:{second_job_id} succeeded"


def test_two_jobs_good_bad():
    job_set_name = f"test-{uuid.uuid1()}"

    first_job = no_auth_client.submit_jobs(
        queue="test",
        job_set_id=job_set_name,
        job_request_items=sleep_pod(image="busybox"),
    )
    first_job_id = first_job.job_response_items[0].job_id

    job_state, job_message = search_for_job_complete(
        job_service_client=job_service_client,
        armada_queue="test",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=first_job_id,
    )
    assert job_state == JobState.SUCCEEDED
    assert job_message == f"Armada test:{first_job_id} succeeded"

    second_job = no_auth_client.submit_jobs(
        queue="test",
        job_set_id=job_set_name,
        job_request_items=sleep_pod(image="nonexistant"),
    )
    second_job_id = second_job.job_response_items[0].job_id

    job_state, job_message = search_for_job_complete(
        job_service_client=job_service_client,
        armada_queue="test",
        job_set_id=job_set_name,
        airflow_task_name="test",
        job_id=second_job_id,
    )
    assert job_state == JobState.FAILED
    assert job_message.startswith(f"Armada test:{second_job_id} failed")
