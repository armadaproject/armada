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

with DAG(
    dag_id='hello_with_armada_imports',
    start_date=pendulum.datetime(2016, 1, 1, tz="UTC"),
    schedule_interval='@daily',
    catchup=False,
    default_args={'retries': 2},
) as dag:

    op = BashOperator(task_id='dummy', bash_command='echo Hello World!')
    op_2 = BashOperator(task_id='dummy_2', bash_command='echo Hello World!')
    op >> op_2
