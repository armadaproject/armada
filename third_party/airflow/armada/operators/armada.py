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
import tenacity
from airflow.configuration import conf
from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.models.taskinstance import TaskInstance
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

from .errors import ArmadaOperatorJobFailedError
from ..hooks import ArmadaHook
from ..model import RunningJobContext
from ..policies.reattach import external_job_uri, policy
from ..triggers import ArmadaPollJobTrigger
from ..utils import log_exceptions, xcom_pull_for_ti, resolve_parameter_value


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
:param reattach_policy: Operator reattach policy to use (defaults to: never)
:type reattach_policy: Optional[str] | Callable[[JobState, str], bool]
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
        poll_interval: int = conf.getint(
            "armada_operator", "default_poll_interval_s", fallback=30
        ),
        container_logs: Optional[str] = None,
        k8s_token_retriever: Optional[TokenRetriever] = None,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=True
        ),
        job_acknowledgement_timeout: int = conf.getint(
            "armada_operator", "default_job_acknowledgement_timeout_s", fallback=5 * 60
        ),
        dry_run: bool = conf.getboolean(
            "armada_operator", "default_dry_run", fallback=False
        ),
        reattach_policy: Optional[str] | Callable[[JobState, str], bool] = None,
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

        if reattach_policy is callable(reattach_policy):
            self.log.info(
                f"Configured reattach policy with callable',"
                f" max retries: {self.retries}"
            )
            self.reattach_policy = reattach_policy
        else:
            configured_reattach_policy: str = resolve_parameter_value(
                "reattach_policy", reattach_policy, kwargs, "never"
            )
            self.log.info(
                f"Configured reattach policy to: '{configured_reattach_policy}',"
                f" max retries: {self.retries}"
            )
            self.reattach_policy = policy(configured_reattach_policy)

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

        self.log.info(
            "ArmadaOperator("
            f"deferrable: {self.deferrable}, "
            f"dry_run: {self.dry_run}, "
            f"poll_interval: {self.poll_interval}s, "
            f"job_acknowledgement_timeout: {self.job_acknowledgement_timeout}s)"
        )

        # We take the job_set_id from Airflow's run_id.
        # So all jobs in the dag will be in the same jobset.
        self.job_set_id = f"{self.job_set_prefix}{context['run_id']}"

        if self.dry_run:
            self.log.info(
                f"Running in dry_run mode. job_set_id: {self.job_set_id} \n"
                f"{self.job_request}"
            )
            return

        if self.deferrable:
            # We push channel args via xcom - so we do it once per execution.
            self._xcom_push(context, key="channel_args", value=self.channel_args)

        # Submit job or reattach to previously submitted job.
        # Always do this synchronously.
        self.job_context = self._reattach_or_submit_job(
            context, self.job_set_id, self.job_request
        )

        self._poll_for_termination(context)

    @property
    def hook(self) -> ArmadaHook:
        return ArmadaHook(self.channel_args)

    @property
    def pod_manager(self) -> KubernetesPodLogManager:
        return KubernetesPodLogManager(token_retriever=self.k8s_token_retriever)

    @tenacity.retry(
        wait=tenacity.wait_random_exponential(max=3),
        stop=tenacity.stop_after_attempt(5),
        retry=tenacity.retry_if_not_exception_type(jinja2.TemplateSyntaxError),
        reraise=True,
    )
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

        xcom_job_request = xcom_pull_for_ti(context["ti"], key="job_request")
        if xcom_job_request:
            self.job_request = xcom_job_request
            super().render_template_fields(context, jinja_env)
        else:
            self.log.info("Rendering job_request")
            if callable(self.job_request):
                if not jinja_env:
                    jinja_env = self.get_template_env()
                self.job_request = self.job_request(context, jinja_env)

            if type(self.job_request) is JobSubmitRequestItem:
                self.job_request = MessageToDict(
                    self.job_request, preserving_proto_field_name=True
                )
            super().render_template_fields(context, jinja_env)
            self._xcom_push(context, key="job_request", value=self.job_request)

        self.job_request = ParseDict(self.job_request, JobSubmitRequestItem())
        self._annotate_job_request(context, self.job_request)

        # We want to disable log-fetching if container name is incorrect
        if self.container_logs and self.job_request.pod_spec:
            containers = {c.name for c in self.job_request.pod_spec.containers}
            if self.container_logs not in containers:
                self.log.error(
                    f"unable to fetch logs as {self.container_logs} is not present "
                    f"in job_request. Available containers: {containers}"
                )
                self.container_logs = None

    def on_kill(self) -> None:
        if self.job_context is not None:
            self.log.info(
                f"on_kill called, "
                f"cancelling Armada job with job-id {self.job_context.job_id} in queue "
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
                    DateTime.utcnow() + datetime.timedelta(seconds=self.poll_interval)
                ),
                method_name="_trigger_reentry",
            )
        else:
            time.sleep(self.poll_interval)

    def _trigger_reentry(
        self, context: Context, event: Tuple[str, Dict[str, Any]]
    ) -> None:
        self.job_context = self.hook.context_from_xcom(context["ti"])
        if not self.job_context:
            self.job_context = deserialize(event)
        self._poll_for_termination(context)

    def _reattach_or_submit_job(
        self,
        context: Context,
        job_set_id: str,
        job_request: JobSubmitRequestItem,
    ) -> RunningJobContext:
        ctx = self._try_reattach_to_running_job(context)
        if ctx:
            self.log.info(
                "Attached to existing Armada job "
                f"with job-id {ctx.job_id}."
                f" {self._trigger_tracking_message(ctx.job_id)}"
            )
            return ctx

        # We haven't got a running job, submit a new one and persist state to xcom.
        ctx = self.hook.submit_job(self.armada_queue, job_set_id, job_request)
        tracking_msg = self._trigger_tracking_message(ctx.job_id)
        self.log.info(
            f"Submitted job to Armada with job-id {ctx.job_id}. {tracking_msg}"
        )

        self.hook.context_to_xcom(context["ti"], ctx, self.lookout_url(ctx.job_id))

        return ctx

    def _try_reattach_to_running_job(
        self, context: Context
    ) -> Optional[RunningJobContext]:
        # On first try we intentionally do not re-attach.
        new_run = (
            context["ti"].max_tries - context["ti"].try_number + 1
            == context["ti"].task.retries
        )
        if new_run:
            return None

        expected_job_uri = external_job_uri(context)
        ctx = self.hook.job_by_external_job_uri(
            self.armada_queue, self.job_set_id, expected_job_uri
        )

        if ctx:
            termination_reason = self.hook.job_termination_reason(ctx)
            if self.reattach_policy(ctx.state, termination_reason):
                return ctx
            else:
                self.log.info(
                    f"Found: job-id {ctx.job_id} in {ctx.state}. "
                    "Didn't reattach due to reattach policy."
                )

        return None

    def _poll_for_termination(self, context: Context) -> None:
        while self.job_context.state.is_active():
            self._check_job_status_and_fetch_logs(context)
            if self.job_context.state.is_active():
                self._yield()

        self._running_job_terminated(context["ti"], self.job_context)

    def _running_job_terminated(self, ti: TaskInstance, context: RunningJobContext):
        self.log.info(
            f"job {context.job_id} terminated with state: {context.state.name}"
        )
        if context.state != JobState.SUCCEEDED:
            error = ArmadaOperatorJobFailedError(
                context.armada_queue,
                context.job_id,
                context.state,
                self.hook.job_termination_reason(context),
            )
            if self.reattach_policy(error.state, error.reason):
                self.log.error(str(error))
                raise AirflowFailException()
            else:
                raise error

    def _not_acknowledged_within_timeout(self) -> bool:
        if self.job_context.state == JobState.UNKNOWN:
            if (
                DateTime.utcnow().diff(self.job_context.submit_time).in_seconds()
                > self.job_acknowledgement_timeout
            ):
                return True
        return False

    def _should_have_a_pod_in_k8s(self) -> bool:
        return self.job_context.state in {
            JobState.RUNNING,
            JobState.FAILED,
            JobState.SUCCEEDED,
        }

    @log_exceptions
    def _check_job_status_and_fetch_logs(self, context) -> None:
        self.job_context = self.hook.refresh_context(
            self.job_context, self._trigger_tracking_message(self.job_context.job_id)
        )

        if self._not_acknowledged_within_timeout():
            self.log.info(
                f"Armada job with job-id: {self.job_context.job_id} not acknowledged "
                f"within timeout ({self.job_acknowledgement_timeout}), terminating"
            )
            self.job_context = self.hook.cancel_job(self.job_context)
            return

        if self._should_have_a_pod_in_k8s() and self.container_logs:
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

        self.hook.context_to_xcom(
            context["ti"], self.job_context, self.lookout_url(self.job_context.job_id)
        )

    @tenacity.retry(
        wait=tenacity.wait_random_exponential(max=3),
        stop=tenacity.stop_after_attempt(5),
        reraise=True,
    )
    @log_exceptions
    def _xcom_push(self, context, key: str, value: Any):
        task_instance = context["ti"]
        task_instance.xcom_push(key=key, value=value)

    def _annotate_job_request(self, context, request: JobSubmitRequestItem):
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
        request.annotations[annotation_key_prefix + "externalJobUri"] = (
            external_job_uri(context)
        )
