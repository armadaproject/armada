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

import asyncio
import datetime
import functools
import os
import threading
import time
from dataclasses import dataclass
from functools import cached_property
from typing import Any, AsyncIterator, Dict, Optional, Sequence, Tuple

import jinja2
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context
from airflow.utils.log.logging_mixin import LoggingMixin
from armada.auth import TokenRetriever
from armada.log_manager import KubernetesPodLogManager
from armada.model import GrpcChannelArgs
from armada_client.armada.job_pb2 import JobRunDetails
from armada_client.armada.submit_pb2 import JobSubmitRequestItem
from armada_client.client import ArmadaClient
from armada_client.typings import JobState
from google.protobuf.json_format import MessageToDict, ParseDict
from pendulum import DateTime


def log_exceptions(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as e:
            if hasattr(self, "log") and hasattr(self.log, "error"):
                self.log.error(f"Exception in {method.__name__}: {e}")
            raise

    return wrapper


@dataclass(frozen=False)
class _RunningJobContext:
    armada_queue: str
    job_set_id: str
    job_id: str
    state: JobState = JobState.UNKNOWN
    start_time: DateTime = DateTime.utcnow()
    cluster: Optional[str] = None
    last_log_time: Optional[DateTime] = None

    def serialize(self) -> tuple[str, Dict[str, Any]]:
        return (
            "armada.operators.armada._RunningJobContext",
            {
                "armada_queue": self.armada_queue,
                "job_set_id": self.job_set_id,
                "job_id": self.job_id,
                "state": self.state.value,
                "start_time": self.start_time,
                "cluster": self.cluster,
                "last_log_time": self.last_log_time,
            },
        )

    def from_payload(payload: Dict[str, Any]) -> _RunningJobContext:
        return _RunningJobContext(
            armada_queue=payload["armada_queue"],
            job_set_id=payload["job_set_id"],
            job_id=payload["job_id"],
            state=JobState(payload["state"]),
            start_time=payload["start_time"],
            cluster=payload["cluster"],
            last_log_time=payload["last_log_time"],
        )


class _ArmadaPollJobTrigger(BaseTrigger):
    def __init__(self, moment: datetime.timedelta, context: _RunningJobContext) -> None:
        super().__init__()
        self.moment = moment
        self.context = context

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "armada.operators.armada._ArmadaPollJobTrigger",
            {"moment": self.moment, "context": self.context.serialize()},
        )

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, _ArmadaPollJobTrigger):
            return False
        return self.moment == value.moment and self.context == value.context

    async def run(self) -> AsyncIterator[TriggerEvent]:
        while self.moment > DateTime.utcnow():
            await asyncio.sleep(1)
        yield TriggerEvent(self.context)


class _ArmadaClientFactory:
    CLIENTS_LOCK = threading.Lock()
    CLIENTS: Dict[str, ArmadaClient] = {}

    @staticmethod
    def client_for(args: GrpcChannelArgs) -> ArmadaClient:
        """
        Armada clients, maintain GRPC connection to Armada API. 
        We want to setup as few of them as possible, instead of one per long-running job.
        We cache them in class level cache. 
        
        Access to this method can be from multiple-threads.
        """
        channel_args_key = str(args.serialize())
        with _ArmadaClientFactory.CLIENTS_LOCK:
            if channel_args_key not in _ArmadaClientFactory.CLIENTS:
                _ArmadaClientFactory.CLIENTS[channel_args_key] = ArmadaClient(
                    channel=args.channel()
                )
            return _ArmadaClientFactory.CLIENTS[channel_args_key]


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
        k8s_token_retriever: Optional[TokenRetriever] = None,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=True
        ),
        job_acknowledgement_timeout: int = 5 * 60,
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
        # We take the job_set_id from Airflow's run_id. This means that all jobs in the
        # dag will be in the same jobset.
        self.job_set_id = f"{self.job_set_prefix}{context['run_id']}"

        self._annotate_job_request(context, self.job_request)

        # Submit job or reattach to previously submitted job. We always do this
        # synchronously.
        job_id = self._reattach_or_submit_job(
            context, self.armada_queue, self.job_set_id, self.job_request
        )

        # Wait until finished
        self.job_context = _RunningJobContext(
            self.armada_queue, self.job_set_id, job_id, start_time=DateTime.utcnow()
        )
        if self.deferrable:
            self._deffered_yield(self.job_context)
        else:
            self._poll_for_termination(self.job_context)

    @cached_property
    def client(self) -> ArmadaClient:
        return _ArmadaClientFactory.client_for(self.channel_args)

    @cached_property
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
        self.job_request = MessageToDict(
            self.job_request, preserving_proto_field_name=True
        )
        super().render_template_fields(context, jinja_env)
        self.job_request = ParseDict(self.job_request, JobSubmitRequestItem())

    def _cancel_job(self, job_context) -> None:
        try:
            result = self.client.cancel_jobs(
                queue=job_context.armada_queue,
                job_set_id=job_context.job_set_id,
                job_id=job_context.job_id,
            )
            if len(list(result.cancelled_ids)) > 0:
                self.log.info(f"Cancelled job with id {result.cancelled_ids}")
            else:
                self.log.warning(f"Failed to cancel job with id {job_context.job_id}")
        except Exception as e:
            self.log.warning(f"Failed to cancel job with id {job_context.job_id}: {e}")

    def on_kill(self) -> None:
        if self.job_context is not None:
            self.log.info(
                f"on_kill called, "
                "cancelling job with id {self.job_context.job_id} in queue "
                f"{self.job_context.armada_queue}"
            )
            self._cancel_job(self.job_context)

    def _trigger_tracking_message(self, job_id: str):
        if self.lookout_url_template:
            return (
                f"Job details available at "
                f'{self.lookout_url_template.replace("<job_id>", job_id)}'
            )

        return ""

    def _deffered_yield(self, context: _RunningJobContext):
        self.defer(
            timeout=self.execution_timeout,
            trigger=_ArmadaPollJobTrigger(
                DateTime.utcnow() + datetime.timedelta(seconds=self.poll_interval),
                context,
            ),
            method_name="_deffered_poll_for_termination",
        )

    @log_exceptions
    def _deffered_poll_for_termination(
        self, context: Context, event: Tuple[str, Dict[str, Any]]
    ) -> None:
        job_run_context = _RunningJobContext.from_payload(event[1])
        while job_run_context.state.is_active():
            job_run_context = self._check_job_status_and_fetch_logs(job_run_context)
            if job_run_context.state.is_active():
                self._deffered_yield(job_run_context)

        self._running_job_terminated(job_run_context)

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
                f"Attached to existing job with id {existing_id['armada_job_id']}."
                f" {self._trigger_tracking_message(existing_id['armada_job_id'])}"
            )
            return existing_id["armada_job_id"]

        job_id = self._submit_job(queue, job_set_id, job_request)
        self.log.info(
            f"Submitted job with id {job_id}. {self._trigger_tracking_message(job_id)}"
        )
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

    def _poll_for_termination(self, context: _RunningJobContext) -> None:
        while context.state.is_active():
            context = self._check_job_status_and_fetch_logs(context)
            if context.state.is_active():
                time.sleep(self.poll_interval)

        self._running_job_terminated(context)

    def _running_job_terminated(self, context: _RunningJobContext):
        self.log.info(
            f"job {context.job_id} terminated with state: {context.state.name}"
        )
        if context.state != JobState.SUCCEEDED:
            raise AirflowException(
                f"job {context.job_id} did not succeed. "
                f"Final status was {context.state.name}"
            )

    @log_exceptions
    def _check_job_status_and_fetch_logs(
        self, context: _RunningJobContext
    ) -> _RunningJobContext:
        response = self.client.get_job_status([context.job_id])
        state = JobState(response.job_states[context.job_id])
        if state != context.state:
            self.log.info(
                f"job {context.job_id} is in state: {state.name}. "
                f"{self._trigger_tracking_message(context.job_id)}"
            )
        context.state = state

        if context.state == JobState.UNKNOWN:
            if (
                DateTime.utcnow().diff(context.start_time).in_seconds()
                > self.job_acknowledgement_timeout
            ):
                self.log.info(
                    f"Job {context.job_id} not acknowledged by the Armada within "
                    f"timeout ({self.job_acknowledgement_timeout}), terminating"
                )
                self._cancel_job(context)
                context.state = JobState.CANCELLED
                return context

        if self.container_logs and not context.cluster:
            if context.state == JobState.RUNNING or context.state.is_terminal():
                run_details = self._get_latest_job_run_details(context.job_id)
                context.cluster = run_details.cluster

        if context.cluster:
            try:
                context.last_log_time = self.pod_manager.fetch_container_logs(
                    k8s_context=context.cluster,
                    namespace=self.job_request.namespace,
                    pod=f"armada-{context.job_id}-0",
                    container=self.container_logs,
                    since_time=context.last_log_time,
                )
            except Exception as e:
                self.log.warning(f"Error fetching logs {e}")
        return context

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
