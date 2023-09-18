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
            priority=1,
            pod_spec=pod,
            namespace="personal-anonymous",
            annotations={"armadaproject.io/hello": "world"},
        )
    ]


"""
This is an example of a Airflow dag that uses a BashOperator and an ArmadaOperator
"""
with DAG(
    dag_id="big_armada",
    start_date=pendulum.datetime(2016, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2},
) as dag:
    """
    The ArmadaDeferrableOperator requires grpc.channel arguments for armada and
    the jobservice.
    """
    armada_channel_args = {"target": "127.0.0.1:50051"}
    job_service_channel_args = {"target": "127.0.0.1:60003"}

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
    armada1 = ArmadaOperator(
        task_id="armada1",
        name="armada1",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada2 = ArmadaOperator(
        task_id="armada2",
        name="armada2",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada3 = ArmadaOperator(
        task_id="armada3",
        name="armada3",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada4 = ArmadaOperator(
        task_id="armada4",
        name="armada4",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada5 = ArmadaOperator(
        task_id="armada5",
        name="armada5",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada6 = ArmadaOperator(
        task_id="armada6",
        name="armada6",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada7 = ArmadaOperator(
        task_id="armada7",
        name="armada7",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada8 = ArmadaOperator(
        task_id="armada8",
        name="armada8",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada9 = ArmadaOperator(
        task_id="armada9",
        name="armada9",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada10 = ArmadaOperator(
        task_id="armada10",
        name="armada10",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada11 = ArmadaOperator(
        task_id="armada11",
        name="armada11",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada12 = ArmadaOperator(
        task_id="armada12",
        name="armada12",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )
    armada13 = ArmadaOperator(
        task_id="armada13",
        name="armada13",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada14 = ArmadaOperator(
        task_id="armada14",
        name="armada14",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada15 = ArmadaOperator(
        task_id="armada15",
        name="armada15",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada16 = ArmadaOperator(
        task_id="armada16",
        name="armada16",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada17 = ArmadaOperator(
        task_id="armada17",
        name="armada17",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada18 = ArmadaOperator(
        task_id="armada18",
        name="armada18",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )
    armada19 = ArmadaOperator(
        task_id="armada19",
        name="armada19",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    armada20 = ArmadaOperator(
        task_id="armada20",
        name="armada20",
        armada_queue="test",
        job_service_channel_args=job_service_channel_args,
        armada_channel_args=armada_channel_args,
        job_request_items=submit_sleep_job(),
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
    )

    """
    Airflow dag syntax for running op and then armada.
    """
    op >> [
        armada1,
        armada2,
        armada3,
        armada4,
        armada5,
        armada6,
        armada7,
        armada8,
        armada9,
        armada10,
        armada11,
        armada12,
        armada13,
        armada14,
        armada14,
        armada15,
        armada16,
        armada17,
        armada18,
        armada19,
        armada20,
    ]
