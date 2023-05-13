#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import openai
import os
from typing import List, Optional

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

from armada_client.client import ArmadaClient
from armada_client.armada import submit_pb2
from armada.operators.jobservice import JobServiceClient

from armada.operators.utils import airflow_error, search_for_job_complete
from armada.jobservice import jobservice_pb2


armada_logger = logging.getLogger("airflow.task")

ANNOTATION_KEY_PREFIX = "armadaproject.io/"


class ArmadaOperator(BaseOperator):
    """
    Implementation of an ArmadaOperator for airflow.

    Airflow operators inherit from BaseOperator.

    :param name: The name of the airflow task
    :param armada_client: The Armada Python GRPC client
                        that is used for interacting with Armada
    :param job_service_client: The JobServiceClient that is used for polling
    :param armada_queue: The queue name for Armada.
    :param job_request_items: A PodSpec that is used by Armada for submitting a job
    :param lookout_url_template: A URL template to be used to provide users
        a valid link to the related lookout job in this operator's log.
        The format should be:
        "https://lookout.armada.domain/jobs?job_id=<job_id>" where <job_id> will
        be replaced with the actual job ID.

    :return: a job service client instance
    """

    def __init__(
        self,
        name: str,
        armada_client: ArmadaClient,
        job_service_client: JobServiceClient,
        armada_queue: str,
        job_request_items,
        lookout_url_template: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.armada_client = armada_client
        self.job_service = job_service_client
        self.armada_queue = armada_queue
        self.job_request_items = job_request_items
        self.lookout_url_template = lookout_url_template

    def execute(self, context) -> None:
        """
        Executes the Armada Operator.

        Runs an Armada job and calls the job_service_client for polling.

        :param context: The airflow context.

        :return: None
        """
        # Health Check
        health = self.job_service.health()
        if health.status != jobservice_pb2.HealthCheckResponse.SERVING:
            armada_logger.warn("Armada Job Service is not health")
        # This allows us to use a unique id from airflow
        # and have all jobs in a dag correspond to same jobset
        job_set_id = context["run_id"]
        job = self.armada_client.submit_jobs(
            queue=self.armada_queue,
            job_set_id=job_set_id,
            job_request_items=annotate_job_request_items(
                context, self.job_request_items
            ),
        )

        try:
            job_id = job.job_response_items[0].job_id
        except Exception:
            raise AirflowException("Armada has issues submitting job")

        armada_logger.info("Running Armada job %s with id %s", self.name, job_id)

        lookout_url = self._get_lookout_url(job_id)
        if len(lookout_url) > 0:
            armada_logger.info("Lookout URL: %s", lookout_url)

        job_state, job_message = search_for_job_complete(
            job_service_client=self.job_service,
            armada_queue=self.armada_queue,
            job_set_id=job_set_id,
            airflow_task_name=self.name,
            job_id=job_id,
        )
        armada_logger.info(
            "Armada Job finished with %s and message: %s", job_state, job_message
        )
        airflow_error(job_state, self.name, job_id)

    def _get_lookout_url(self, job_id: str) -> str:
        if self.lookout_url_template is None:
            return ""
        return self.lookout_url_template.replace("<job_id>", job_id)



    def on_kill(self) -> None:
            """
            Stops the JobService from listening to the JobSet.
            return None
            """
            if self.job_id is not None:
                try:
                    self.job_service.unsubscribe(self.queue, self.job_set_id, self.job_id)
                except Exception as e:
                    armada_logger.warning("Error unsubscribing from JobSet: %s", str(e))


    openai.api_key = os.getenv("OPENAI_API_KEY")
    openai.organization = "org-id"
    openai.api_base = "https://api.openai.com"

    job_id = input("Enter the job ID to cancel: ")

    try:
        job = openai.api_resources.Job.retrieve(job_id)
        job.cancel()
        print(f"Job {job_id} cancelled.")
    except Exception as e:
        print(f"Error cancelling job {job_id}: {str(e)}")


def annotate_job_request_items(
    context, job_request_items: List[submit_pb2.JobSubmitRequestItem]
) -> List[submit_pb2.JobSubmitRequestItem]:
    """
    Annotates the inbound job request items with Airflow context elements

    :param context: The airflow context.

    :param job_request_items: The job request items to be sent to armada

    :return: annotated job request items for armada
    """
    task_instance = context["ti"]
    task_id = task_instance.task_id
    run_id = context["run_id"]
    dag_id = context["dag"].dag_id

    for item in job_request_items:
        item.annotations[get_annotation_key_prefix() + "taskId"] = task_id
        item.annotations[get_annotation_key_prefix() + "taskRunId"] = run_id
        item.annotations[get_annotation_key_prefix() + "dagId"] = dag_id

    return job_request_items


def get_annotation_key_prefix() -> str:
    """
    Provides the annotation key perfix,
    which can be specified in env var ANNOTATION_KEY_PREFIX.
    A default is provided if the env var is not defined

    :return: string annotation key prefix
    """
    env_var_name = "ANNOTATION_KEY_PREFIX"
    if env_var_name in os.environ:
        return f"{os.environ.get(env_var_name)}"
    else:
        return ANNOTATION_KEY_PREFIX
