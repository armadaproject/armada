from airflow import DAG
from airflow.operators.bash import BashOperator
from armada.operators.armada import ArmadaOperator

from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)

from armada_client.armada import (
    submit_pb2,
)

from armada_client.client import ArmadaClient
import grpc

import pendulum

from armada.operators.jobservice import (
    JobServiceClient,
    get_retryable_job_service_client,
)


def submit_sleep_container(image: str):
    """
    Simple armada job where image allows you to control
    if this container fails or not.
    """
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="sleep",
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

    return [
        submit_pb2.JobSubmitRequestItem(
            priority=1, pod_spec=pod, namespace="personal-anonymous"
        )
    ]


with DAG(
    dag_id="error_armada",
    start_date=pendulum.datetime(2016, 1, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    default_args={"retries": 2},
) as dag:
    """
    This Airflow DAG follows a similar pattern.
    1) Create your clients and jobservice clients.
    2) Define your ArmadaOperator tasks that you want to run
    3) Generate a DAG definition
    """
    no_auth_client = ArmadaClient(
        channel=grpc.insecure_channel(target="127.0.0.1:50051")
    )
    job_service_client = get_retryable_job_service_client(target="127.0.0.1:60003")

    op = BashOperator(task_id="dummy", bash_command="echo Hello World!")
    armada = ArmadaOperator(
        task_id="armada",
        name="armada",
        armada_queue="test",
        job_service_client=job_service_client,
        armada_client=no_auth_client,
        job_request_items=submit_sleep_container(image="busybox"),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )
    """
    This task is used to verify that if an Armada Job
    fails we are correctly telling Airflow that it failed.
    """
    bad_armada = ArmadaOperator(
        task_id="armada_fail",
        name="armada_fail",
        armada_queue="test",
        job_service_client=job_service_client,
        armada_client=no_auth_client,
        job_request_items=submit_sleep_container(image="nonexistant"),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )
    good_armada = ArmadaOperator(
        task_id="good_armada",
        name="good_armada",
        armada_queue="test",
        job_service_client=job_service_client,
        armada_client=no_auth_client,
        job_request_items=submit_sleep_container(image="busybox"),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )
    """
    Airflow syntax to say
    Run op first and then run armada and bad_armada in parallel
    If all jobs are successful, run good_armada.
    """
    op >> [armada, bad_armada] >> good_armada
