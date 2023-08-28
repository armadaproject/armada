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
from typing import Optional, Sequence, List

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context

from armada_client.armada.submit_pb2 import JobSubmitRequestItem
from armada_client.client import ArmadaClient

from armada.operators.jobservice import (
    JobServiceClient,
    default_jobservice_channel_options,
)
from armada.operators.grpc import GrpcChannelArgsDict, GrpcChannelArguments
from armada.operators.jobservice_asyncio import JobServiceAsyncIOClient
from armada.operators.utils import (
    airflow_error,
    search_for_job_complete_async,
    annotate_job_request_items,
)
from armada.jobservice import jobservice_pb2

from google.protobuf.json_format import MessageToDict, ParseDict

import jinja2


armada_logger = logging.getLogger("airflow.task")


class ArmadaDeferrableOperator(BaseOperator):
    """
    Implementation of a deferrable armada operator for airflow.

    Distinguished from ArmadaOperator by its ability to defer itself after
    submitting its job_request_items.

    See
    https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html
    for more information about deferrable airflow operators.

    Airflow operators inherit from BaseOperator.

    :param name: The name of the airflow task.
    :param armada_channel_args: GRPC channel arguments to be used when creating
      a grpc channel to connect to the armada server instance.
    :param job_service_channel_args: GRPC channel arguments to be used when creating
      a grpc channel to connect to the job service instance.
    :param armada_queue: The queue name for Armada.
    :param job_request_items: A PodSpec that is used by Armada for submitting a job.
    :param lookout_url_template: A URL template to be used to provide users
        a valid link to the related lookout job in this operator's log.
        The format should be:
        "https://lookout.armada.domain/jobs?job_id=<job_id>" where <job_id> will
        be replaced with the actual job ID.
    :param poll_interval: How often to poll jobservice to get status.
    :return: A deferrable armada operator instance.
    """

    template_fields: Sequence[str] = ("job_request_items",)

    def __init__(
        self,
        name: str,
        armada_channel_args: GrpcChannelArgsDict,
        job_service_channel_args: GrpcChannelArgsDict,
        armada_queue: str,
        job_request_items: List[JobSubmitRequestItem],
        lookout_url_template: Optional[str] = None,
        poll_interval: int = 30,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.armada_channel_args = GrpcChannelArguments(**armada_channel_args)

        if "options" not in job_service_channel_args:
            job_service_channel_args["options"] = default_jobservice_channel_options

        self.job_service_channel_args = GrpcChannelArguments(**job_service_channel_args)
        self.armada_queue = armada_queue
        self.job_request_items = job_request_items
        self.lookout_url_template = lookout_url_template
        self.poll_interval = poll_interval

    def execute(self, context) -> None:
        """
        Executes the Armada Operator. Only meant to be called by airflow.

        Submits an Armada job and defers itself to ArmadaJobCompleteTrigger to wait
        until the job completes.

        :param context: The airflow context.

        :return: None
        """
        self.job_request_items = annotate_job_request_items(
            context=context, job_request_items=self.job_request_items
        )
        job_service_client = JobServiceClient(self.job_service_channel_args.channel())

        # Health Check
        health = job_service_client.health()
        if health.status != jobservice_pb2.HealthCheckResponse.SERVING:
            armada_logger.warn("Armada Job Service is not healthy.")
        else:
            armada_logger.debug("Jobservice is healthy.")

        armada_client = ArmadaClient(channel=self.armada_channel_args.channel())

        armada_logger.debug("Submitting job(s).")
        # This allows us to use a unique id from airflow
        # and have all jobs in a dag correspond to same jobset
        job = armada_client.submit_jobs(
            queue=self.armada_queue,
            job_set_id=context["run_id"],
            job_request_items=self.job_request_items,
        )

        try:
            job_id = job.job_response_items[0].job_id
        except Exception:
            raise AirflowException("Error submitting job(s) to Armada")

        armada_logger.info("Running Armada job %s with id %s", self.name, job_id)

        lookout_url = self._get_lookout_url(job_id)
        if len(lookout_url) > 0:
            armada_logger.info("Lookout URL: %s", lookout_url)

        # TODO: configurable timeout?
        self.defer(
            trigger=ArmadaJobCompleteTrigger(
                job_id=job_id,
                job_service_channel_args=self.job_service_channel_args.serialize(),
                armada_queue=self.armada_queue,
                job_set_id=context["run_id"],
                airflow_task_name=self.name,
            ),
            method_name="resume_job_complete",
            kwargs={"job_id": job_id},
        )

    def resume_job_complete(self, context, event: dict, job_id: str) -> None:
        """
        Resumes this operator after deferring itself to ArmadaJobCompleteTrigger.
        Only meant to be called from within Airflow.

        Reports the result of the job and returns.

        :param context: The airflow context.
        :param event: The payload from the TriggerEvent raised by
            ArmadaJobCompleteTrigger.
        :param job_id: The job ID.
        :return: None
        """

        job_state = event["job_state"]
        job_message = event["job_message"]

        armada_logger.info(
            "Armada Job finished with %s and message: %s", job_state, job_message
        )
        airflow_error(job_state, self.name, job_id)

    def _get_lookout_url(self, job_id: str) -> str:
        if self.lookout_url_template is None:
            return ""
        return self.lookout_url_template.replace("<job_id>", job_id)

    def render_template_fields(
        self,
        context: Context,
        jinja_env: Optional[jinja2.Environment] = None,
    ) -> None:
        self.job_request_items = [
            MessageToDict(x, preserving_proto_field_name=True)
            for x in self.job_request_items
        ]
        super().render_template_fields(context, jinja_env)
        self.job_request_items = [
            ParseDict(x, JobSubmitRequestItem()) for x in self.job_request_items
        ]


class ArmadaJobCompleteTrigger(BaseTrigger):
    """
    An airflow trigger that monitors the job state of an armada job.

    Triggers when the job is complete.

    :param job_id: The job ID to monitor.
    :param job_service_channel_args: GRPC channel arguments to be used when
      creating a grpc channel to connect to the job service instance.
    :param armada_queue: The name of the armada queue.
    :param job_set_id: The ID of the job set.
    :param airflow_task_name: Name of the airflow task to which this trigger
      belongs.
    :return: An armada job complete trigger instance.
    """

    def __init__(
        self,
        job_id: str,
        job_service_channel_args: GrpcChannelArgsDict,
        armada_queue: str,
        job_set_id: str,
        airflow_task_name: str,
    ) -> None:
        super().__init__()
        self.job_id = job_id
        self.job_service_channel_args = GrpcChannelArguments(**job_service_channel_args)
        self.armada_queue = armada_queue
        self.job_set_id = job_set_id
        self.airflow_task_name = airflow_task_name

    def serialize(self) -> tuple:
        return (
            "armada.operators.armada_deferrable.ArmadaJobCompleteTrigger",
            {
                "job_id": self.job_id,
                "job_service_channel_args": self.job_service_channel_args.serialize(),
                "armada_queue": self.armada_queue,
                "job_set_id": self.job_set_id,
                "airflow_task_name": self.airflow_task_name,
            },
        )

    async def run(self):
        """
        Runs the trigger. Meant to be called by an airflow triggerer process.
        """
        job_service_client = JobServiceAsyncIOClient(
            channel=self.job_service_channel_args.aio_channel()
        )

        job_state, job_message = await search_for_job_complete_async(
            job_service_client=job_service_client,
            armada_queue=self.armada_queue,
            job_set_id=self.job_set_id,
            airflow_task_name=self.airflow_task_name,
            job_id=self.job_id,
            poll_interval=self.poll_interval,
            log=self.log,
        )
        yield TriggerEvent({"job_state": job_state, "job_message": job_message})
