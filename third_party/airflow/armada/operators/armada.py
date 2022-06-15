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

from typing import TYPE_CHECKING, Any, Callable, List, Optional, Sequence

from airflow.models import BaseOperator

from armada_client.client import ArmadaClient, unwatch_events

if TYPE_CHECKING:
    from airflow.utils.context import Context
import logging


armada_logger = logging.getLogger('airflow.task')


class ArmadaOperator(BaseOperator):
    def __init__(self, name: str, armada_client: ArmadaClient, queue: str, job_set_id: str, job_request_items, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.armada_client = armada_client
        self.queue = queue
        self.job_set_id = job_set_id
        self.job_request_items = job_request_items

    def execute(self, context):

        # get the airflow.task logger
        message = f"Hello {self.name}"
        armada_logger.info("Running Armada Operator")
        job = self.armada_client.submit_jobs(queue=self.queue, job_set_id=self.job_set_id, job_request_items = self.job_request_items)

        job_id = job.job_response_items[0].job_id

        sleep_event = self.armada_client.get_job_events_stream(queue=self.queue, job_set_id=self.job_set_id)
        search_for_successful_event(sleep_event, self.name, job_id)
        unwatch_events(sleep_event)

        return message


def search_for_successful_event(event, job_name: str, completed_job: str):
    for element in event:
        if element.message.succeeded.job_id == completed_job:
            armada_logger.info(
                f'Armada job {job_name} with id {completed_job} completed without error')
            break
    return
