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

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

from armada_client.client import ArmadaClient
from armada.operators.jobservice import JobServiceClient

import logging

from armada.operators.utils import airflow_error, search_for_job_complete
from armada.jobservice import jobservice_pb2

armada_logger = logging.getLogger("airflow.task")


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

    :return: a job service client instance
    """

    def __init__(
        self,
        name: str,
        armada_client: ArmadaClient,
        job_service_client: JobServiceClient,
        armada_queue: str,
        job_request_items,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.armada_client = armada_client
        self.job_service = job_service_client
        self.armada_queue = armada_queue
        self.job_request_items = job_request_items

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
            job_request_items=self.job_request_items,
        )

        try:
            job_id = job.job_response_items[0].job_id
            armada_logger.info("Running Armada job %s with id %s", self.name, job_id)
        except Exception:
            raise AirflowException("Armada has issues submitting job")

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
