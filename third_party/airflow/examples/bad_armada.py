import airflow
from datetime import datetime, timedelta
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

from armada_client.client import ArmadaClient, unwatch_events
import grpc

import pendulum

def submit_sleep_job():
    pod = core_v1.PodSpec(containers=[
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

    return [submit_pb2.JobSubmitRequestItem(priority=1, pod_spec=pod)]

def submit_bad_job():
    pod = core_v1.PodSpec(containers=[
        core_v1.Container(
            name="echo",
            image="NOTCONTAINTER",
            args=["echo", "hello"],
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

with DAG(
    dag_id='error_armada',
    start_date=pendulum.datetime(2016, 1, 1, tz="UTC"),
    schedule_interval='@daily',
    catchup=False,
    default_args={'retries': 2},
) as dag:
    no_auth_client = ArmadaClient(channel=grpc.insecure_channel(target="127.0.0.1:50051"))

    op = BashOperator(task_id='dummy', bash_command='echo Hello World!')
    armada = ArmadaOperator(task_id='armada', name='armada', queue='test', job_set_id='job-set-1', armada_client=no_auth_client, job_request_items = submit_sleep_job())
    bad_armada = ArmadaOperator(task_id='armada_fail', name='armada_fail', queue='test', job_set_id='job-set-1', armada_client=no_auth_client, job_request_items = submit_bad_job())
    op >> [armada, bad_armada] >> armada
