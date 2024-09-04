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
from __future__ import annotations

import dataclasses
import datetime
import os
import time
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

import jinja2
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.serialization.serde import deserialize
from airflow.utils.context import Context
from airflow.utils.log.logging_mixin import LoggingMixin
from armada.auth import TokenRetriever
from armada.log_manager import KubernetesPodLogManager
from armada.model import GrpcChannelArgs
from armada_client.armada.submit_pb2 import JobSubmitRequestItem
from armada_client.typings import JobState
from google.protobuf.json_format import MessageToDict, ParseDict
from pendulum import DateTime

from ..hooks import ArmadaHook
from ..model import RunningJobContext
from ..triggers import ArmadaPollJobTrigger
from ..utils import log_exceptions


class LookoutLink(BaseOperatorLink):
    name = "Lookout"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
        task_state = XCom.get_value(ti_key=ti_key)
        if not task_state:
            return ""

        return task_state.get("armada_lookout_url", "")


class ArmadaOperator(BaseOperator, LoggingMixin):
    """
    An Airflow operator that manages Job submission to Armada.

    This operator submits a job to an Armada cluster, polls for its completion,
    and handles job cancellation if the Airflow task is killed.
    """

    operator_extra_links = (LookoutLink(),)

    template_fields: Sequence[str] = ("job_request", "job_set_prefix")
    template_fields_renderers: Dict[str, str] = {"job_request": "py"}

    """
Initializes a new ArmadaOperator.

:param name: The name of the job to be submitted.
:type name: str
:param channel_args: The gRPC channel arguments for connecting to the Armada server.
:type channel_args: GrpcChannelArgs
:param armada_queue: The name of the Armada queue to which the job will be submitted.
:type armada_queue: str
:param job_request: The job to be submitted to Armada.
:type job_request: JobSubmitRequestItem | \
Callable[[Context, jinja2.Environment], JobSubmitRequestItem]
:param job_set_prefix: A string to prepend to the jobSet name.
:type job_set_prefix: Optional[str]
:param lookout_url_template: Template for creating lookout links. If not specified
then no tracking information will be logged.
:type lookout_url_template: Optional[str]
:param poll_interval: The interval in seconds between polling for job status updates.
:type poll_interval: int
:param container_logs: Name of container whose logs will be published to stdout.
:type container_logs: Optional[str]
:param k8s_token_retriever: A serialisable Kubernetes token retriever object. We use
this to read logs from Kubernetes pods.
:type k8s_token_retriever: Optional[TokenRetriever]
:param deferrable: Whether the operator should run in a deferrable mode, allowing
for asynchronous execution.
:type deferrable: bool
:param job_acknowledgement_timeout: The timeout in seconds to wait for a job to be
acknowledged by Armada.
:type job_acknowledgement_timeout: int
:param dry_run: Run Operator in dry-run mode - render Armada request and terminate.
:type dry_run: bool
:param kwargs: Additional keyword arguments to pass to the BaseOperator.
"""

    def __init__(
        self,
        name: str,
        channel_args: GrpcChannelArgs,
        armada_queue: str,
        job_request: (
            JobSubmitRequestItem
            | Callable[[Context, jinja2.Environment], JobSubmitRequestItem]
        ),
        job_set_prefix: Optional[str] = "",
        lookout_url_template: Optional[str] = None,
        poll_interval: int = 30,
        container_logs: Optional[str] = None,
        k8s_token_retriever: Optional[TokenRetriever] = None,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=True
        ),
        job_acknowledgement_timeout: int = 5 * 60,
        dry_run: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.channel_args = channel_args
        self.armada_queue = armada_queue
        self.job_request = job_request
        self.job_set_id = None
        self.job_set_prefix = job_set_prefix
        self.lookout_url_template = lookout_url_template
        self.poll_interval = poll_interval
        self.container_logs = container_logs
        self.k8s_token_retriever = k8s_token_retriever
        self.deferrable = deferrable
        self.job_acknowledgement_timeout = job_acknowledgement_timeout
        self.dry_run = dry_run
        self.job_context = None

        if self.container_logs and self.k8s_token_retriever is None:
            self.log.warning(
                "Token refresh mechanism not configured, airflow may stop retrieving "
                "logs from Kubernetes"
            )

    @log_exceptions
    def execute(self, context) -> None:
        """
        Submits the job to Armada and polls for completion.

        :param context: The execution context provided by Airflow.
        :type context: Context
        """
        # We take the job_set_id from Airflow's run_id.
        # So all jobs in the dag will be in the same jobset.
        self.job_set_id = f"{self.job_set_prefix}{context['run_id']}"

        self._annotate_job_request(context, self.job_request)

        if self.dry_run:
            self.log.info(
                f"Running in dry_run mode. job_set_id: {self.job_set_id} \n"
                f"{self.job_request}"
            )
            return

        # Submit job or reattach to previously submitted job.
        # Always do this synchronously.
        self.job_context = self._reattach_or_submit_job(
            context, self.job_set_id, self.job_request
        )
        self._poll_for_termination()

    @property
    def hook(self) -> ArmadaHook:
        return ArmadaHook(self.channel_args)

    @property
    def pod_manager(self) -> KubernetesPodLogManager:
        return KubernetesPodLogManager(token_retriever=self.k8s_token_retriever)

    @log_exceptions
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
        :param context: Airflow Context dict wi1th values to apply on content
        :param jinja_env: jinja’s environment to use for rendering.
        """
        if callable(self.job_request):
            if not jinja_env:
                jinja_env = self.get_template_env()
            self.job_request = self.job_request(context, jinja_env)

        self.job_request = MessageToDict(
            self.job_request, preserving_proto_field_name=True
        )
        super().render_template_fields(context, jinja_env)
        self.job_request = ParseDict(self.job_request, JobSubmitRequestItem())

    def on_kill(self) -> None:
        if self.job_context is not None:
            self.log.info(
                f"on_kill called, "
                f"cancelling job with id {self.job_context.job_id} in queue "
                f"{self.job_context.armada_queue}"
            )
            self.hook.cancel_job(self.job_context)
            self.job_context = None

    def lookout_url(self, job_id):
        if self.lookout_url_template:
            return self.lookout_url_template.replace("<job_id>", job_id)
        return None

    def _trigger_tracking_message(self, job_id):
        url = self.lookout_url(job_id)
        if url:
            return f"Job details available at {url}"

        return ""

    def _yield(self):
        if self.deferrable:
            self.defer(
                timeout=self.execution_timeout,
                trigger=ArmadaPollJobTrigger(
                    DateTime.utcnow() + datetime.timedelta(seconds=self.poll_interval),
                    self.job_context,
                    self.channel_args,
                ),
                method_name="_trigger_reentry",
            )
        else:
            time.sleep(self.poll_interval)

    def _trigger_reentry(
        self, context: Context, event: Tuple[str, Dict[str, Any]]
    ) -> None:
        self.job_context = deserialize(event)
        self._poll_for_termination()

    def _reattach_or_submit_job(
        self,
        context: Context,
        job_set_id: str,
        job_request: JobSubmitRequestItem,
    ) -> RunningJobContext:
        # Try to re-initialize job_context from xcom if it exist.
        ti = context["ti"]
        existing_run = ti.xcom_pull(
            dag_id=ti.dag_id, task_ids=ti.task_id, key=f"{ti.try_number}"
        )
        if existing_run is not None:
            self.log.info(
                f"Attached to existing job with id {existing_run['armada_job_id']}."
                f" {self._trigger_tracking_message(existing_run['armada_job_id'])}"
            )
            return RunningJobContext(
                armada_queue=existing_run["armada_queue"],
                job_id=existing_run["armada_job_id"],
                job_set_id=existing_run["armada_job_set_id"],
                submit_time=DateTime.utcnow(),
            )

        # We haven't got a running job, submit a new one and persist state to xcom.
        ctx = self.hook.submit_job(self.armada_queue, job_set_id, job_request)
        tracking_msg = self._trigger_tracking_message(ctx.job_id)
        self.log.info(f"Submitted job with id {ctx.job_id}. {tracking_msg}")

        ti.xcom_push(
            key=f"{ti.try_number}",
            value={
                "armada_queue": ctx.armada_queue,
                "armada_job_id": ctx.job_id,
                "armada_job_set_id": ctx.job_set_id,
                "armada_lookout_url": self.lookout_url(ctx.job_id),
            },
        )
        return ctx

    def _poll_for_termination(self) -> None:
        while self.job_context.state.is_active():
            self._check_job_status_and_fetch_logs()
            if self.job_context.state.is_active():
                self._yield()

        self._running_job_terminated(self.job_context)

    def _running_job_terminated(self, context: RunningJobContext):
        self.log.info(
            f"job {context.job_id} terminated with state: {context.state.name}"
        )
        if context.state != JobState.SUCCEEDED:
            raise AirflowException(
                f"job {context.job_id} did not succeed. "
                f"Final status was {context.state.name}"
            )

    def _not_acknowledged_within_timeout(self) -> bool:
        if self.job_context.state == JobState.UNKNOWN:
            if (
                DateTime.utcnow().diff(self.job_context.submit_time).in_seconds()
                > self.job_acknowledgement_timeout
            ):
                return True
        return False

    @log_exceptions
    def _check_job_status_and_fetch_logs(self) -> None:
        self.job_context = self.hook.refresh_context(
            self.job_context, self._trigger_tracking_message(self.job_context.job_id)
        )

        if self._not_acknowledged_within_timeout():
            self.log.info(
                f"Job {self.job_context.job_id} not acknowledged by the Armada within "
                f"timeout ({self.job_acknowledgement_timeout}), terminating"
            )
            self.job_context = self.hook.cancel_job(self.job_context)
            return

        if self.job_context.cluster and self.container_logs:
            try:
                last_log_time = self.pod_manager.fetch_container_logs(
                    k8s_context=self.job_context.cluster,
                    namespace=self.job_request.namespace,
                    pod=f"armada-{self.job_context.job_id}-0",
                    container=self.container_logs,
                    since_time=self.job_context.last_log_time,
                )
                if last_log_time:
                    self.job_context = dataclasses.replace(
                        self.job_context, last_log_time=last_log_time
                    )
            except Exception as e:
                self.log.warning(f"Error fetching logs {e}")

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
