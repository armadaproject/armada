from airflow.models.taskinstance import TaskInstance
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)

import grpc
from concurrent import futures
from armada_client.armada import submit_pb2_grpc, submit_pb2, event_pb2_grpc

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
    job_request_items = sleep_job()
    task_id = "58896abbfr9"
    operator = ArmadaOperator(
        task_id=task_id,
        name="armada-task",
        armada_queue="test",
        job_service_client=job_service_client,
        armada_client=no_auth_client,
        job_request_items=job_request_items,
    )

    task_instance = TaskInstance(operator)
    context = {"ti": task_instance}

    result = annotate_job_request_items(context, job_request_items)
    assert result[0][0].annotations == {"armadaproject.io/taskId": task_id}
