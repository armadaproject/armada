# armada-airflow-operator

Armada Airflow Operator, which manages airflow jobs. This allows Armada jobs to be run as part of an Airflow DAG

## Overview

The `ArmadaOperator` allows user to run an Armada Job as a task in an Airflow DAG.   It handles job submission, job
state management and (optionally) log streaming back to Airflow.

The Operator works by periodically polling Armada for the state of each job.  As a result, it is only intended for DAGs
with tens or (at the limit) hundreds of concurrent jobs.

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

from armada.operators.armada import ArmadaOperator

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
                        "cpu": api_resource.Quantity(string="1"),
                        "memory": api_resource.Quantity(string="1Gi"),
                    },
                    limits={
                        "cpu": api_resource.Quantity(string="1"),
                        "memory": api_resource.Quantity(string="1Gi"),
                    },
                ),
            )
        ],
    )

    return submit_pb2.JobSubmitRequestItem(
        priority=1, pod_spec=pod, namespace="armada"
    )

armada_channel_args = {"target": "127.0.0.1:50051"}


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
        channel_args=armada_channel_args,
        armada_queue="armada",
        job_request=create_dummy_job(),
        container_logs="sleep",
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
        deferrable=False
    )

    armada_deferrable_task = ArmadaOperator(
        name="deferrable_task",
        task_id="2",
        channel_args=armada_channel_args,
        armada_queue="armada",
        job_request=create_dummy_job(),
        container_logs="sleep",
        lookout_url_template="http://127.0.0.1:8089/jobs?job_id=<job_id>",
        deferrable=True
    )

    armada_task >> armada_deferrable_task
```
## Parameters

| Name           | Description                                                                                                                                                          | Notes                                                                                                                                                     |
|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| channel_args   | A list of key-value pairs ([channel_arguments](https://grpc.github.io/grpc/python/glossary.html#term-channel_arguments) in gRPC runtime) to configure the channel.   | None                                                                                                                                                      |                                                              
| armada_queue   | Armada queue to be used for the job                                                                                                                                  | Make sure that Airflow user is permissioned on this queue                                                                                                 |
| job_request    | A `JobSubmitRequestItem` that is to be submitted to Armada as part of this task                                                                                      | Object contains a `core_v1.PodSpec` within it                                                                                                             |
| job_set_prefix | A prefix for the JobSet name provided to Armada when submitting the job                                                                                              | The JobSet name submitted will be the Airflow `run_id` prefixed with this provided prefix                                                                 |
| poll_interval  | Integer number of seconds representing how ofter Airflow will poll Armada for Job Status.  Defaults to 30 Seconds                                                    | Decreasing this makes the operator more responsive but comes at the cost of increased load on the Armada Server. Please do not decrease below 10 seconds. |
| container_logs | Name of the container in your job from which you wish to stream logs.  If unset then no logs will be streamed                                                        | Only use this if you are running relatively few (<50) concurrent jobs                                                                                     |
| deferrable     | Flag to specify whether to run the operator in Airflow Deferrable Mode                                                                                               | Defaults to True                                                                                                                                          |

# Contributing

The [airflow documentation](https://airflow.apache.org/) was used for setting up a simple test server.  

[setup-local-airflow.sh](./setup-local-airflow.sh) demonstrates how to run airflow locally using Airflow's SequentialExecutor.  This is only used for testing purposes.

Adding custom dags requires you to create a ~/airflow/dags folder and copying the dag files under examples in that location.  This allows you to test the DAG in your airflow test server.

## Examples

For documentation by example, see [hello_armada.py](./examples/hello_armada.py) or [bad_armada.py](./examples/bad_armada.py).

## Operator Documentation

[Armada Operator](../../docs/python_airflow_operator.md)

## Usage

The operator is available on [PyPi](https://pypi.org/project/armada-airflow/)

```
python3.8 -m venv armada38
source armada38/bin/activate
python3.8 -m pip install armada-airflow
```

## Development

From the top level of the repo, you should run `make airflow-operator`.  This will generate proto/grpc files in the jobservice folder.

Airflow with the Armada operator can be run alongside the other Armada services via the docker-compose environment. It is manually started in this way:

```
mage airflow start
```

Airflow's web UI will then be accessible at http://localhost:8081/login/  (login with airflow/airflow).

You can install the package via `pip3 install third_party/airflow`. 

You can use our tox file that streamlines development lifecycle.  For development, you can install black, tox, mypy and flake8.

`python3.10 -m tox -e py310` will run unit tests.

`python3.10 -m tox -e format` will run black on your code.

`python3.10 -m tox -e format-check` will run a format check.

`python3.10 -m tox -e docs` will generate a new sphinx doc.

## Releasing the client
Armada-airflow releases are automated via Github Actions, for contributors with sufficient access to run them.

1) Commit and merge a change to `third_party/airflow/pyproject.toml` raising the version number the appropriate amount. We are 
   using [semver](https://semver.org/) for versioning.
2) Navigate to the [airflow operator release workflow](https://github.com/armadaproject/armada/actions/workflows/airflow-operator-release-to-pypi.yml)
   in Github workflows, click the "Run Workflow" button on the right side, and choose "master" as the branch to use the
   workflow from.
3) Once the workflow has completed running, verify the new version of Armada client has been uploaded to
   [PyPI](https://pypi.org/project/armada-airflow/).




