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
from typing import Optional, Sequence, Tuple, Any

import grpc

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent

from armada_client.asyncio_client import ArmadaAsyncIOClient
from armada.operators.jobservice_asyncio import JobServiceAsyncIOClient

from armada.operators.utils import (
    airflow_error,
    search_for_job_complete_async,
    annotate_job_request_items,
)
from armada.jobservice import jobservice_pb2


armada_logger = logging.getLogger("airflow.task")


class GrpcAsyncIOChannelArguments(object):
    """
    A Serializable GRPC Arguments Object.

    """

    def __init__(
        self,
        target: str,
        credentials: Optional[grpc.ChannelCredentials] = None,
        options: Optional[Sequence[Tuple[str, Any]]] = None,
        compression: Optional[grpc.Compression] = None,
        interceptors: Optional[Sequence[grpc.ClientInterceptor]] = None,
    ) -> None:
        self.target = target
        self.credentials = credentials
        self.options = options
        self.compression = compression
        self.interceptors = interceptors

    def instantiate_channel(self) -> grpc.aio.Channel:
        if self.credentials is None:
            return grpc.aio.insecure_channel(
                target=self.target,
                options=self.options,
                compression=self.compression,
                interceptors=self.interceptors,
            )
        return grpc.aio.secure_channel(
            target=self.target,
            credentials=self.credentials,
            options=self.options,
            compression=self.compression,
            interceptors=self.interceptors,
        )

    # TODO: Not sure if this is needed.
    def serialize(self) -> tuple:
        return {
            "target": self.target,
            "credentials": self.credentials,
            "options": self.options,
            "compression": self.compression,
            "interceptors": self.interceptors,
        }.items()


class ArmadaDeferrableOperator(BaseOperator):
    """
    Implementation of an ArmadaDeferrableOperator.

    Distinguished from ArmadaOperator by its ability to defer itself.
    See https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html
    for more information.

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
    """  # noqa

    def __init__(
        self,
        name: str,
        armada_channel_args: GrpcAsyncIOChannelArguments,
        job_service_channel_args: GrpcAsyncIOChannelArguments,
        armada_queue: str,
        job_request_items,
        lookout_url_template: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.armada_channel_args = armada_channel_args
        self.job_service_channel_args = job_service_channel_args
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
        self.job_request_items = annotate_job_request_items(self.job_request_items)
        self.defer(
            trigger=ArmadaSubmitJobTrigger(
                armada_channel_args=self.armada_channel_args,
                job_service_channel_args=self.job_service_channel_args,
                run_id=context["run_id"],
                armada_queue=self.armada_queue,
                job_request_items=self.job_request_items,
            ),
            method_name="resume_submit",
            kwargs={
                "job_service_channel_args": self.job_service_channel_args,
                "armada_queue": self.armada_queue,
                "run_id": context["run_id"],
                "airflow_task_name": self.name,
                "lookout_url_template": self.lookout_url_template,
            },
        )

    def resume_submit(
        self,
        job,
        job_service_channel_args,
        armada_queue: str,
        run_id: str,
        airflow_task_name: str,
        lookout_url_template: Optional[str] = None,
    ) -> None:
        self.lookout_url_template = lookout_url_template

        try:
            job_id = job.job_response_items[0].job_id
        except Exception:
            raise AirflowException("Armada has issues submitting job")

        armada_logger.info("Running Armada job %s with id %s", self.name, job_id)

        lookout_url = self._get_lookout_url(job_id)
        if len(lookout_url) > 0:
            armada_logger.info("Lookout URL: %s", lookout_url)

        # TODO: configurable timeout?
        self.defer(
            trigger=ArmadaJobCompleteTrigger(
                job_id=job_id,
                job_service_channel_args=job_service_channel_args,
                armada_queue=armada_queue,
                job_set_id=run_id,
                airflow_task_name=airflow_task_name,
            ),
            method_name="resume_job_complete",
            kwargs={"job_id": job_id},
        )

    def resume_job_complete(
        self, job_state: str, job_message: str, job_id: str
    ) -> None:
        armada_logger.info(
            "Armada Job finished with %s and message: %s", job_state, job_message
        )
        airflow_error(job_state, self.name, job_id)

    def _get_lookout_url(self, job_id: str) -> str:
        if self.lookout_url_template is None:
            return ""
        return self.lookout_url_template.replace("<job_id>", job_id)


class ArmadaSubmitJobTrigger(BaseTrigger):
    def __init__(
        self,
        armada_channel_args: GrpcAsyncIOChannelArguments,
        job_service_channel_args: GrpcAsyncIOChannelArguments,
        run_id: str,
        armada_queue: str,
        job_request_items,
    ) -> None:
        super().__init__()
        self.armada_channel_args = armada_channel_args
        self.job_service_channel_args = job_service_channel_args
        self.run_id = run_id
        self.armada_queue = armada_queue
        self.job_request_items = job_request_items

    async def run(self) -> TriggerEvent:
        job_service_client = JobServiceAsyncIOClient(
            self.job_service_channel_args.instantiate_channel()
        )

        # Health Check
        health = await job_service_client.health()
        if health.status != jobservice_pb2.HealthCheckResponse.SERVING:
            armada_logger.warn("Armada Job Service is not health")

        armada_client = ArmadaAsyncIOClient(
            self.armada_channel_args.instantiate_channel()
        )

        # This allows us to use a unique id from airflow
        # and have all jobs in a dag correspond to same jobset
        job = await armada_client.submit_jobs(
            queue=self.armada_queue,
            job_set_id=self.run_id,
            job_request_items=self.job_request_items,
        )
        yield TriggerEvent({"job": job})

    def serialize(self) -> tuple:
        return (
            "armada.operators.ArmadaSubmitJobTrigger",
            {
                "armada_channel_args": self.armada_channel_args,
                "job_service_channel_args": self.job_service_channel_args,
                "run_id": self.run_id,
                "armada_queue": self.armada_queue,
                "job_request_items": self.job_request_items,
            },
        )


class ArmadaJobCompleteTrigger(BaseTrigger):
    def __init__(
        self,
        job_id: str,
        job_service_channel_args: GrpcAsyncIOChannelArguments,
        armada_queue: str,
        job_set_id: str,
        airflow_task_name: str,
    ) -> None:
        super().__init__()
        self.job_id = job_id
        self.job_service_channel_args = (job_service_channel_args,)
        self.armada_queue = armada_queue
        self.job_set_id = job_set_id
        self.airflow_task_name = airflow_task_name

    async def run(self) -> TriggerEvent:
        job_service_client = JobServiceAsyncIOClient(
            self.job_service_channel_args.instantiate_channel()
        )

        job_state, job_message = await search_for_job_complete_async(
            job_service_client=job_service_client,
            armada_queue=self.armada_queue,
            job_set_id=self.job_set_id,
            airflow_task_name=self.airflow_task_name,
            job_id=self.job_id,
        )
        yield TriggerEvent({"job_state": job_state, "job_message": job_message})

    def serialize(self) -> tuple:
        return (
            "armada.operators.ArmadaJobCompleteTrigger",
            {
                "job_id": self.job_id,
                "job_service_channel_args": self.job_service_channel_args,
                "armada_queue": self.armada_queue,
                "job_set_id": self.job_set_id,
                "airflow_task_name": self.airflow_task_name,
            },
        )
