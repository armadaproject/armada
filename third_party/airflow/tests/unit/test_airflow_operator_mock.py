from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)

import grpc
from concurrent import futures
from armada_client.armada import submit_pb2_grpc, submit_pb2, event_pb2_grpc

import pytest
from armada.operators.jobservice import JobServiceClient
from armada.operators.utils import JobState, search_for_job_complete
from armada.jobservice import jobservice_pb2_grpc
from armada_client_mock import SubmitService, EventService
from job_service_mock import JobService


@pytest.fixture(scope="session", autouse=True)
def server_mock():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    submit_pb2_grpc.add_SubmitServicer_to_server(SubmitService(), server)
    event_pb2_grpc.add_EventServicer_to_server(EventService(), server)
    server.add_insecure_port("[::]:50052")
    server.start()

    yield
    server.stop(False)


@pytest.fixture(scope="session", autouse=True)
def job_service_mock():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    jobservice_pb2_grpc.add_JobServiceServicer_to_server(JobService(), server)
    server.add_insecure_port("[::]:60008")
    server.start()

    yield
    server.stop(False)


tester_client = ArmadaClient(
    grpc.insecure_channel(
        target="127.0.0.1:50052",
    )
)
tester_jobservice = JobServiceClient(grpc.insecure_channel(target="127.0.0.1:60008"))


def sleep_job():
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="container-1",
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


def test_mock_success_job():

    tester_client.submit_jobs(
        queue="test",
        job_set_id="test",
        job_request_items=sleep_job(),
    )

    job_state, job_message = search_for_job_complete(
        job_service_client=tester_jobservice,
        queue="test",
        job_set_id="test",
        airflow_task_name="test-mock",
        job_id="test_succeeded",
    )
    assert job_state == JobState.SUCCEEDED
    assert job_message == "Armada test-mock:test_succeeded succeeded"


def test_mock_failed_job():

    tester_client.submit_jobs(
        queue="test",
        job_set_id="test",
        job_request_items=sleep_job(),
    )

    job_state, job_message = search_for_job_complete(
        job_service_client=tester_jobservice,
        queue="test",
        job_set_id="test",
        airflow_task_name="test-mock",
        job_id="test_failed",
    )
    assert job_state == JobState.FAILED
    assert job_message.startswith("Armada test-mock:test_failed failed")


def test_mock_cancelled_job():

    tester_client.submit_jobs(
        queue="test",
        job_set_id="test",
        job_request_items=sleep_job(),
    )

    job_state, job_message = search_for_job_complete(
        job_service_client=tester_jobservice,
        queue="test",
        job_set_id="test",
        airflow_task_name="test-mock",
        job_id="test_cancelled",
    )
    assert job_state == JobState.CANCELLED
    assert job_message == "Armada test-mock:test_cancelled cancelled"
