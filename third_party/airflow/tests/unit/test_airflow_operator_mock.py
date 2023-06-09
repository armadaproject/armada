from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
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


def generate_pod_spec(name: str = "container-1") -> core_v1.PodSpec:
    ps = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name=name,
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
    return ps

def sleep_job():
    pod = generate_pod_spec()
    return [submit_pb2.JobSubmitRequestItem(priority=0, pod_spec=pod)]

def pre_template_sleep_job():
    pod = generate_pod_spec(name="name-{{ run_id }}")
    return [submit_pb2.JobSubmitRequestItem(priority=0, pod_spec=pod)]

def expected_sleep_job():
    pod = generate_pod_spec(name="name-another-run-id")
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


def test_parameterize_armada_operator():
    no_auth_client = ArmadaClient(
        channel=grpc.insecure_channel(target="127.0.0.1:50051")
    )
    job_service_client = JobServiceClient(
        channel=grpc.insecure_channel(target="127.0.0.1:60003")
    )

    submitted_job_request_items = pre_template_sleep_job()
    expected_job_request_items = expected_sleep_job()
    task_id = "123456789ab"
    operator = ArmadaOperator(
        task_id=task_id,
        name="armada-task",
        armada_queue="test",
        job_service_client=job_service_client,
        armada_client=no_auth_client,
        job_request_items=submitted_job_request_items,
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
    context = Context(ti=task_instance, dag=dag, run_id="another-run-id")

    assert operator.job_request_items != expected_job_request_items

    operator.render_template_fields(context)

    assert operator.job_request_items == expected_job_request_items
