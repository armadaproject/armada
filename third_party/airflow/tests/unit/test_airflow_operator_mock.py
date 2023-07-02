from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)

import grpc
from concurrent import futures
from armada_client.armada import submit_pb2_grpc, submit_pb2, event_pb2_grpc

import pendulum
import pytest
from armada.operators.armada import ArmadaOperator, annotate_job_request_items
from armada.operators.jobservice import JobServiceClient
from armada.operators.utils import JobState, search_for_job_complete
from armada.jobservice import jobservice_pb2_grpc, jobservice_pb2
from armada_client_mock import SubmitService, EventService
from job_service_mock import JobService


@pytest.fixture(scope="session", autouse=True)
def server_mock():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    submit_pb2_grpc.add_SubmitServicer_to_server(SubmitService(), server)
    event_pb2_grpc.add_EventServicer_to_server(EventService(), server)
    server.add_insecure_port("[::]:50099")
    server.start()

    yield
    server.stop(False)


@pytest.fixture(scope="session", autouse=True)
def job_service_mock():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    jobservice_pb2_grpc.add_JobServiceServicer_to_server(JobService(), server)
    server.add_insecure_port("[::]:60081")
    server.start()

    yield
    server.stop(False)


tester_client = ArmadaClient(
    grpc.insecure_channel(
        target="127.0.0.1:50099",
    )
)
tester_jobservice = JobServiceClient(grpc.insecure_channel(target="127.0.0.1:60081"))


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


def test_job_service_health():
    health = tester_jobservice.health()
    assert health.status == jobservice_pb2.HealthCheckResponse.SERVING


def test_mock_success_job():
    tester_client.submit_jobs(
        queue="test",
        job_set_id="test",
        job_request_items=sleep_job(),
    )

    job_state, job_message = search_for_job_complete(
        job_service_client=tester_jobservice,
        armada_queue="test",
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
        armada_queue="test",
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
        armada_queue="test",
        job_set_id="test",
        airflow_task_name="test-mock",
        job_id="test_cancelled",
    )
    assert job_state == JobState.CANCELLED
    assert job_message == "Armada test-mock:test_cancelled cancelled"


def test_annotate_job_request_items():
    no_auth_client = ArmadaClient(
        channel=grpc.insecure_channel(target="127.0.0.1:50051")
    )
    job_service_client = JobServiceClient(
        channel=grpc.insecure_channel(target="127.0.0.1:60003")
    )

    job_request_items = sleep_job()
    task_id = "58896abbfr9"
    operator = ArmadaOperator(
        task_id=task_id,
        name="armada-task",
        armada_queue="test",
        job_service_client=job_service_client,
        armada_client=no_auth_client,
        job_request_items=job_request_items,
        lookout_url_template="http://127.0.0.1:8089",
    )

    task_instance = TaskInstance(operator)
    dag = DAG(
        dag_id="hello_armada",
        start_date=pendulum.datetime(2016, 1, 1, tz="UTC"),
        schedule_interval="@daily",
        catchup=False,
        default_args={"retries": 2},
    )
    context = {"ti": task_instance, "dag": dag, "run_id": "some-run-id"}

    result = annotate_job_request_items(context, job_request_items)
    assert result[0].annotations == {
        "armadaproject.io/taskId": task_id,
        "armadaproject.io/taskRunId": "some-run-id",
        "armadaproject.io/dagId": "hello_armada",
    }


import unittest
from armada.operators import ArmadaOperator
from armada_client.client import ArmadaClient

class TestArmadaOperatorIntegration(unittest.TestCase):
    def test_on_kill(self):
        armada_client = ArmadaClient()  # Initialize the Armada client
        job_service_client = MagicMock()  # Mock the Jobservice client
        armada_queue = "armada_queue"
        job_request_items = [...]  # Define the job request items

        operator = ArmadaOperator(
            name="test_operator",
            armada_client=armada_client,
            job_service_client=job_service_client,
            armada_queue=armada_queue,
            job_request_items=job_request_items,
            poll_interval=30,
        )

        # Execute the operator's on_kill() method
        operator.on_kill()

        # Assert the expected interactions between Armada and Jobservice
        job_service_client.cancel_job.assert_called_once_with(
            job_set_id=operator.job_set_id, queue=armada_queue
        )

if __name__ == "__main__":
    unittest.main()
