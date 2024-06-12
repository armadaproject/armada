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

import os
import time
from functools import cache, cached_property
from typing import Optional, Sequence, Any

import jinja2
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from airflow.utils.context import Context
from airflow.utils.log.logging_mixin import LoggingMixin
from armada_client.armada.job_pb2 import JobRunDetails
from armada_client.typings import JobState
from armada_client.armada.submit_pb2 import JobSubmitRequestItem
from google.protobuf.json_format import MessageToDict, ParseDict

from armada_client.client import ArmadaClient
from armada.auth import TokenRetriever
from armada.logs.pod_log_manager import PodLogManager
from armada.model import GrpcChannelArgs
from armada.triggers.armada import ArmadaTrigger


class ArmadaOperator(BaseOperator, LoggingMixin):
    """
    An Airflow operator that manages Job submission to Armada.

    This operator submits a job to an Armada cluster, polls for its completion,
    and handles job cancellation if the Airflow task is killed.
    """

    template_fields: Sequence[str] = ("job_request", "job_set_prefix")

    """
Initializes a new ArmadaOperator.

:param name: The name of the job to be submitted.
:type name: str
:param channel_args: The gRPC channel arguments for connecting to the Armada server.
:type channel_args: GrpcChannelArgs
:param armada_queue: The name of the Armada queue to which the job will be submitted.
:type armada_queue: str
:param job_request: The job to be submitted to Armada.
:type job_request: JobSubmitRequestItem
:param job_set_prefix: A string to prepend to the jobSet name
:type job_set_prefix: Optional[str]
:param lookout_url_template: Template for creating lookout links. If not specified
then no tracking information will be logged.
:type lookout_url_template: Optional[str]
:param poll_interval: The interval in seconds between polling for job status updates.
:type poll_interval: int
:param container_logs: Name of container whose logs will be published to stdout.
:type container_logs: Optional[str]
:param deferrable: Whether the operator should run in a deferrable mode, allowing
for asynchronous execution.
:type deferrable: bool
:param job_acknowledgement_timeout: The timeout in seconds to wait for a job to be
acknowledged by Armada.
:type job_acknowledgement_timeout: int
:param kwargs: Additional keyword arguments to pass to the BaseOperator.
"""

    def __init__(
        self,
        name: str,
        channel_args: GrpcChannelArgs,
        armada_queue: str,
        job_request: JobSubmitRequestItem,
        job_set_prefix: Optional[str] = "",
        lookout_url_template: Optional[str] = None,
        poll_interval: int = 30,
        container_logs: Optional[str] = None,
        token_retriever: Optional[TokenRetriever] = None,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        job_acknowledgement_timeout: int = 5 * 60,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.channel_args = channel_args
        self.armada_queue = armada_queue
        self.job_request = job_request
        self.job_set_prefix = job_set_prefix
        self.lookout_url_template = lookout_url_template
        self.poll_interval = poll_interval
        self.container_logs = container_logs
        self.token_retriever = token_retriever
        self.deferrable = deferrable
        self.job_acknowledgement_timeout = job_acknowledgement_timeout
        self.job_id = None

        if self.container_logs and self.token_retriever is None:
            self.log.warning(
                "Token refresh mechanism not configured, airflow may stop retrieving "
                "logs from Kubernetes"
            )

    def execute(self, context) -> None:
        """
        Submits the job to Armada and polls for completion.

        :param context: The execution context provided by Airflow.
        :type context: Context
        """
        # We take the job_set_id from Airflow's run_id. This means that all jobs in the
        # dag will be in the same jobset.
        job_set_id = f"{self.job_set_prefix}{context['run_id']}"
        self._annotate_job_request(context, self.job_request)

        # Submit job or reattach to previously submitted job. We always do this
        # synchronously.
        self.job_id = self._reattach_or_submit_job(
            context, self.armada_queue, job_set_id, self.job_request
        )

        # Wait until finished
        if self.deferrable:
            self.defer(
                timeout=self.execution_timeout,
                trigger=ArmadaTrigger(
                    job_id=self.job_id,
                    channel_args=self.channel_args,
                    poll_interval=self.poll_interval,
                    tracking_message=self._trigger_tracking_message(),
                    job_acknowledgement_timeout=self.job_acknowledgement_timeout,
                    container_logs=self.container_logs,
                    token_retriever=self.token_retriever,
                    job_request_namespace=self.job_request.namespace,
                ),
                method_name="_execute_complete",
            )
        else:
            self._poll_for_termination(self._trigger_tracking_message())

    @cached_property
    def client(self) -> ArmadaClient:
        return ArmadaClient(channel=self.channel_args.channel())

    @cache
    def pod_manager(self, k8s_context: str) -> PodLogManager:
        return PodLogManager(
            k8s_context=k8s_context, token_retriever=self.token_retriever
        )

    def render_template_fields(
        self,
        context: Context,
        jinja_env: Optional[jinja2.Environment] = None,
    ) -> None:
        """
        Template all attributes listed in self.template_fields.
        This mutates the attributes in-place and is irreversible.

        Args:
            context (Context): The execution context provided by Airflow.
        :param context: Airflow Context dict with values to apply on content
        :param jinja_env: jinja’s environment to use for rendering.
        """
        self.job_request = MessageToDict(
            self.job_request, preserving_proto_field_name=True
        )
        super().render_template_fields(context, jinja_env)
        self.job_request = ParseDict(self.job_request, JobSubmitRequestItem())

    def _cancel_job(self) -> None:
        try:
            result = self.client.cancel_jobs(
                job_id=self.job_id,
            )
            if len(list(result.cancelled_ids)) > 0:
                self.log.info(f"Cancelled job with id {result.cancelled_ids}")
            else:
                self.log.warning(f"Failed to cancel job with id {self.job_id}")
        except Exception as e:
            self.log.warning(f"Failed to cancel job with id {self.job_id}: {e}")

    def on_kill(self) -> None:
        if self.job_id is not None:
            self.log.info(
                f"on_kill called, cancelling job with id {self.job_id} in queue "
                f"{self.armada_queue}"
            )
            self._cancel_job()

    def _trigger_tracking_message(self):
        if self.lookout_url_template:
            return (
                f"Job details available at "
                f'{self.lookout_url_template.replace("<job_id>", self.job_id)}'
            )

        return ""

    def _execute_complete(self, _: Context, event: dict[str, Any]):
        if event["status"] == "error":
            raise AirflowException(event["response"])

    def _reattach_or_submit_job(
        self,
        context: Context,
        queue: str,
        job_set_id: str,
        job_request: JobSubmitRequestItem,
    ) -> str:
        ti = context["ti"]
        existing_id = ti.xcom_pull(
            dag_id=ti.dag_id, task_ids=ti.task_id, key=f"{ti.try_number}"
        )
        if existing_id is not None:
            self.log.info(
                f"Attached to existing job with id {existing_id['armada_job_id']}"
            )
            return existing_id["armada_job_id"]

        job_id = self._submit_job(queue, job_set_id, job_request)
        self.log.info(f"Submitted job with id {job_id}")
        ti.xcom_push(key=f"{ti.try_number}", value={"armada_job_id": job_id})
        return job_id

    def _submit_job(
        self, queue: str, job_set_id: str, job_request: JobSubmitRequestItem
    ) -> str:
        resp = self.client.submit_jobs(queue, job_set_id, [job_request])
        num_responses = len(resp.job_response_items)

        # We submitted exactly one job to armada, so we expect a single response
        if num_responses != 1:
            raise AirflowException(
                f"No valid received from Armada (expected 1 job to be created "
                f"but got {num_responses}"
            )
        job = resp.job_response_items[0]

        # Throw if armada told us we had submitted something bad
        if job.error:
            raise AirflowException(f"Error submitting job to Armada: {job.error}")

        return job.job_id

    def _poll_for_termination(self, tracking_message: str) -> None:
        last_log_time = None
        run_details = None
        state = JobState.UNKNOWN

        start_time = time.time()
        job_acknowledged = False
        while state.is_active():
            tmp = self.client.get_job_status([self.job_id]).job_states
            print(tmp)
            print(self.job_id)
            state = JobState(tmp[self.job_id])
            self.log.info(
                f"job {self.job_id} is in state: {state.name}. {tracking_message}"
            )

            if state != JobState.UNKNOWN:
                job_acknowledged = True

            if (
                not job_acknowledged
                and int(time.time() - start_time) > self.job_acknowledgement_timeout
            ):
                self.log.info(
                    f"Job {self.job_id} not acknowledged by the Armada server within "
                    f"timeout ({self.job_acknowledgement_timeout}), terminating"
                )
                self.on_kill()
                return

            if self.container_logs and not run_details:
                if state == JobState.RUNNING or state.is_terminal():
                    run_details = self._get_latest_job_run_details(self.job_id)

            if run_details:
                try:
                    # pod_name format is sufficient for now. Ideally pod name should be
                    # retrieved from queryapi
                    log_status = self.pod_manager(
                        run_details.cluster
                    ).fetch_container_logs(
                        pod_name=f"armada-{self.job_id}-0",
                        namespace=self.job_request.namespace,
                        container_name=self.container_logs,
                        since_time=last_log_time,
                    )
                    last_log_time = log_status.last_log_time
                except Exception as e:
                    self.log.warning(f"Error fetching logs {e}")

            time.sleep(self.poll_interval)

        self.log.info(f"job {self.job_id} terminated with state: {state.name}")
        if state != JobState.SUCCEEDED:
            raise AirflowException(
                f"job {self.job_id} did not succeed. Final status was {state.name}"
            )

    def _get_latest_job_run_details(self, job_id) -> Optional[JobRunDetails]:
        job_details = self.client.get_job_details([job_id]).job_details[job_id]
        if job_details and job_details.latest_run_id:
            for run in job_details.job_runs:
                if run.run_id == job_details.latest_run_id:
                    return run
        return None

    @staticmethod
    def _annotate_job_request(context, request: JobSubmitRequestItem):
        if "ANNOTATION_KEY_PREFIX" in os.environ:
            annotation_key_prefix = f'{os.environ.get("ANNOTATION_KEY_PREFIX")}'
        else:
            annotation_key_prefix = "armadaproject.io/"

        task_id = context["ti"].task_id
        run_id = context["run_id"]
        dag_id = context["dag"].dag_id

        request.annotations[annotation_key_prefix + "taskId"] = task_id
        request.annotations[annotation_key_prefix + "taskRunId"] = run_id
        request.annotations[annotation_key_prefix + "dagId"] = dag_id
