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

from armada_client.client import ArmadaClient, unwatch_events, search_for_job_complete

import logging

from armada.operators.utils import airflow_error

armada_logger = logging.getLogger("airflow.task")


class ArmadaOperator(BaseOperator):
    def __init__(
        self,
        name: str,
        armada_client: ArmadaClient,
        queue: str,
        job_set_id: str,
        job_request_items,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.armada_client = armada_client
        self.queue = queue
        self.job_set_id = job_set_id
        self.job_request_items = job_request_items

    def execute(self, context):

        # get the airflow.task logger
        job = self.armada_client.submit_jobs(
            queue=self.queue,
            job_set_id=self.job_set_id,
            job_request_items=self.job_request_items,
        )

        try:
            job_id = job.job_response_items[0].job_id
            armada_logger.info(f"Running Armada job {self.name} with id {job_id}")
        except:
            raise AirflowException("Armada has issues submitting job")
        job_events = self.armada_client.get_job_events_stream(
            queue=self.queue, job_set_id=self.job_set_id
        )
        job_state, job_message = search_for_job_complete(job_events, self.name, job_id)
        armada_logger.info(
            f"Armada Job finished with {job_state} and message: {job_message}"
        )
        airflow_error(job_state, self.name, job_id)
        unwatch_events(job_events)

        return job_message
