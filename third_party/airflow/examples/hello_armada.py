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
from armada.operators.jobservice import JobServiceClient


def submit_sleep_job():
    """
    This is a PodSpec definition that allows you to run sleep.
    This returns an array of JobSubmitRequestItems that allows you
    to submit to Armada.
    """
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="sleep",
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

    return [
        submit_pb2.JobSubmitRequestItem(
            priority=1, pod_spec=pod, namespace="personal-anonymous"
        )
    ]


"""
This is an example of a Airflow dag that uses a BashOperator and an ArmadaOperator
"""
with DAG(
    dag_id="hello_armada",
    start_date=pendulum.datetime(2016, 1, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    default_args={"retries": 2},
) as dag:
    """
    The ArmadaOperator requires a python client and a JobServiceClient
    so we initialize them and set up their channel arguments.
    """
    no_auth_client = ArmadaClient(
        channel=grpc.insecure_channel(target="127.0.0.1:50051")
    )
    job_service_client = JobServiceClient(
        channel=grpc.insecure_channel(target="127.0.0.1:60003")
    )
    """
    This defines an Airflow task that runs Hello World and it gives the airflow
    task name of dummy.
    """
    op = BashOperator(task_id="dummy", bash_command="echo Hello World!")
    """
    This is creating an Armada task with the task_id of armada and name of armada.
    The Airflow operator needs queue and job-set for Armada
    You also specify the PythonClient and JobServiceClient for each task.
    You should reuse them for all your tasks.
    This job will use the podspec defined above.
    """
    armada = ArmadaOperator(
        task_id="armada",
        name="armada",
        armada_queue="test",
        job_service_client=job_service_client,
        armada_client=no_auth_client,
        job_request_items=submit_sleep_job(),
    )
    """
    Airflow dag syntax for running op and then armada.
    """
    op >> armada
