from airflow import DAG
from airflow.operators.bash import BashOperator
from armada.operators.armada_deferrable import ArmadaDeferrableOperator

from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)

from armada_client.armada import (
    submit_pb2,
)

from armada.operators.jobservice import default_jobservice_channel_options


import pendulum


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
                image="localhost:5051/busybox",
                args=["sleep", "20s"],
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
            priority=1,
            pod_spec=pod,
            namespace="personal-anonymous",
            annotations={"armadaproject.io/hello": "world"},
        )
    ]


"""
This is an example of a Airflow dag that uses a BashOperator and
an ArmadaDeferrableOperator
"""
with DAG(
    dag_id="hello_armada_deferrable",
    start_date=pendulum.datetime(2016, 1, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    default_args={"retries": 2},
) as dag:
    """
    The ArmadaDeferrableOperatorOperator requires grpc.aio.channel arguments
    """
    armada_channel_args = {"target": "127.0.0.1:50051"}
    job_service_channel_args = {
        "target": "127.0.0.1:60003",
        "options": default_jobservice_channel_options,
    }
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
    armada = ArmadaDeferrableOperator(
        task_id="armada_deferrable",
        name="armada_deferrable",
        armada_channel_args=armada_channel_args,
        job_service_channel_args=job_service_channel_args,
        armada_queue="test",
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )
    """
    Airflow dag syntax for running op and then armada.
    """
    op >> armada
