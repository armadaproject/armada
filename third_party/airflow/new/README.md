# armada-airflow-operator
 
Armada Airflow Operator, which manages airflow jobs. This allows Armada jobs to be run as part of an Airflow DAG

## Overview

The `ArmadaOperator` allows user to run an Armada Job as a task in an Airflow DAG.   It handles job submission, job
state management and (optionally) log streaming back to Airflow. 

The Operator works by periodically polling Armada for the state of each job.  As a result, it is only intended for DAGs
with tens or (at the limit) hundreds of concurrent jobs.

## Getting started

```python

from airflow import DAG
from datetime import datetime

with DAG(
        "example_armada_dag",
        description="Example Armada DAG with helper tooling",
        schedule=None,
        start_date=datetime(2022, 1, 1),
        catchup=False,
        user_defined_macros={
            'version': vid
        }
) as dag:
    # Configure single container "alpine" - dag.
    ArmadaOperator(
        name="EXAMPLE TASK",
        task_id="EXAMPLE_TASK",
        queue="armada",
        job=create_job(
            namespace="armada",
            application_name="alpine",
            # You could also resolve version as below
            image="alpine:3.16.2",
            arguments=["sh", "-c", "echo 'Today is: {{ ds_nodash }}'"],
        ),
        container_logs="alpine",
    )

```

## Installation

`pip install armada-airflow`

## Example Usage

```python
from datetime import datetime

from airflow import DAG
from armada_client.armada import submit_pb2
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)

def create_dummy_job():
    """
    Create a dummy job with a single container.
    """

    # For information on where this comes from,
    # see https://github.com/kubernetes/api/blob/master/core/v1/generated.proto
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="sleep",
                image="alpine:3.16.2",
                args=["sh", "-c", "for i in $(seq 1 60); do echo $i; sleep 1; done"],
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

    return submit_pb2.JobSubmitRequestItem(
        priority=1, pod_spec=pod, namespace="armada"



with DAG(
        "test_new_armada_operator",
        description="Example DAG Showing Usage Of ArmadaOperator",
        schedule=None,
        start_date=datetime(2022, 1, 1),
        catchup=False,
) as dag:
    armada_task = ArmadaOperator(
        name="non_deferrable_task",
        task_id="1",
        queue="armada",
        job=create_dummy_job(),
        container_logs="sleep",
        deferrable=False
    )

    armada_deferrable_task = ArmadaOperator(
        name="deferrable_task",
        task_id="2",
        queue="armada",
        job=create_dummy_job(),
        container_logs="sleep",
        deferrable=True
    )

armada_task >> armada_deferrable_task
```
## Parameters

| Name           | Description                                                                                                                                                                  | Notes                                                                                                                                                    |
|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| queue          | Armada queue to be used for the job                                                                                                                                          | Make sure that Airflow user is permissioned on this queue                                                                                                |
| container_logs | Name of the container in your job from which you wish to stream logs.  If unset then no logs will be streamed                                                                | Only use this if you are running relatively few (<50) concurrent jobs                                                                                    |
| deferrable     | Flag to specify whether to run the operator in Airflow Deferrable Mode                                                                                                       | Defaults to True                                                                                                                                         |
| poll_interval  | Integer number of seconds representing how ofter Airflow will poll Armada for Job Status.  Defaults to 30 Seconds                                                            | Decreasing this makes the operator more responsive but comes at the cost of increased load on the Armada Server. Please do not decrease below 10 seconds. |
| grpc_options   | An optional list of key-value pairs ([channel_arguments](https://grpc.github.io/grpc/python/glossary.html#term-channel_arguments) in gRPC runtime) to configure the channel. | Sensible defaults have been chosen here.  Please only override if you have a good reason!                                                                |                                                              
