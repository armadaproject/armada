# Copyright 2016-2024 The Apache Software Foundation
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
import math
import time
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, cast, Optional

import pendulum
import tenacity
from kubernetes import client, watch, config
from kubernetes_asyncio import client as async_client, config as async_config
from kubernetes.client.rest import ApiException
from pendulum import DateTime
from pendulum.parsing.exceptions import ParserError
from urllib3.exceptions import HTTPError as BaseHTTPError

from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin

from armada.auth import TokenRetriever
from armada.logs.log_consumer import PodLogsConsumer, PodLogsConsumerAsync
from armada.logs.utils import container_is_running

if TYPE_CHECKING:
    from kubernetes.client.models.v1_pod import V1Pod


class PodPhase:
    """
    Possible pod phases.

    See https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase.
    """

    PENDING = "Pending"
    RUNNING = "Running"
    FAILED = "Failed"
    SUCCEEDED = "Succeeded"

    terminal_states = {FAILED, SUCCEEDED}


@dataclass
class PodLoggingStatus:
    """Return the status of the pod and last log time when exiting from `fetch_container_logs`."""

    running: bool
    last_log_time: DateTime | None


class PodLogManagerAsync(LoggingMixin):
    """Monitor logs of Kubernetes pods asynchronously."""

    def __init__(
        self,
        k8s_context: str,
        token_retriever: Optional[TokenRetriever] = None,
    ):
        """
        Create the launcher.

        :param k8s_context: kubernetes context
        :param token_retriever: Retrieves auth tokens
        """
        super().__init__()
        self._k8s_context = k8s_context
        self._watch = watch.Watch()
        self._k8s_client = None
        self._token_retriever = token_retriever

    async def _refresh_k8s_auth_token(self, interval=60 * 5):
        if self._token_retriever is not None:
            while True:
                await asyncio.sleep(interval)
                self._k8s_client.api_client.configuration.api_key["BearerToken"] = (
                    f"Bearer {self._token_retriever.get_token()}"
                )

    async def k8s_client(self) -> async_client:
        await async_config.load_kube_config(context=self._k8s_context)
        asyncio.create_task(self._refresh_k8s_auth_token())
        return async_client.CoreV1Api()

    async def fetch_container_logs(
        self,
        pod_name: str,
        namespace: str,
        container_name: str,
        *,
        follow=False,
        since_time: DateTime | None = None,
        post_termination_timeout: int = 120,
    ) -> PodLoggingStatus:
        """
        Follow the logs of container and stream to airflow logging. Doesn't block whilst logs are being fetched.

        Returns when container exits.

        Between when the pod starts and logs being available, there might be a delay due to CSR not approved
        and signed yet. In such situation, ApiException is thrown. This is why we are retrying on this
        specific exception.
        """
        # Can't await in constructor, so instantiating here
        if self._k8s_client is None:
            self._k8s_client = await self.k8s_client()

        @tenacity.retry(
            retry=tenacity.retry_if_exception_type(ApiException),
            stop=tenacity.stop_after_attempt(10),
            wait=tenacity.wait_fixed(1),
        )
        async def consume_logs(
            *,
            since_time: DateTime | None = None,
            follow: bool = True,
            logs: PodLogsConsumerAsync | None,
        ) -> tuple[DateTime | None, PodLogsConsumerAsync | None]:
            """
            Try to follow container logs until container completes.

            For a long-running container, sometimes the log read may be interrupted
            Such errors of this kind are suppressed.

            Returns the last timestamp observed in logs.
            """
            last_captured_timestamp = None
            try:
                logs = await self._read_pod_logs(
                    pod_name=pod_name,
                    namespace=namespace,
                    container_name=container_name,
                    timestamps=True,
                    since_seconds=(
                        math.ceil((pendulum.now() - since_time).total_seconds())
                        if since_time
                        else None
                    ),
                    follow=follow,
                    post_termination_timeout=post_termination_timeout,
                )
                message_to_log = None
                message_timestamp = None
                progress_callback_lines = []
                try:
                    async for raw_line in logs:
                        line = raw_line.decode("utf-8", errors="backslashreplace")
                        line_timestamp, message = self._parse_log_line(line)
                        if line_timestamp:  # detect new log line
                            if message_to_log is None:  # first line in the log
                                message_to_log = message
                                message_timestamp = line_timestamp
                                progress_callback_lines.append(line)
                            else:  # previous log line is complete
                                self.log.info("[%s] %s", container_name, message_to_log)
                                last_captured_timestamp = message_timestamp
                                message_to_log = message
                                message_timestamp = line_timestamp
                                progress_callback_lines = [line]
                        else:  # continuation of the previous log line
                            message_to_log = f"{message_to_log}\n{message}"
                            progress_callback_lines.append(line)
                finally:
                    if message_to_log is not None:
                        self.log.info("[%s] %s", container_name, message_to_log)
                        last_captured_timestamp = message_timestamp
            except BaseHTTPError as e:
                self.log.warning(
                    "Reading of logs interrupted for container %r with error %r; will retry. "
                    "Set log level to DEBUG for traceback.",
                    container_name,
                    e,
                )
                self.log.debug(
                    "Traceback for interrupted logs read for pod %r",
                    pod_name,
                    exc_info=True,
                )
            return last_captured_timestamp or since_time, logs

        # note: `read_pod_logs` follows the logs, so we shouldn't necessarily *need* to
        # loop as we do here. But in a long-running process we might temporarily lose connectivity.
        # So the looping logic is there to let us resume following the logs.
        logs = None
        last_log_time = since_time
        while True:
            last_log_time, logs = await consume_logs(
                since_time=last_log_time,
                follow=follow,
                logs=logs,
            )
            if not await self._container_is_running_async(
                pod_name, namespace, container_name=container_name
            ):
                return PodLoggingStatus(running=False, last_log_time=last_log_time)
            if not follow:
                return PodLoggingStatus(running=True, last_log_time=last_log_time)
            else:
                self.log.warning(
                    "Pod %s log read interrupted but container %s still running",
                    pod_name,
                    container_name,
                )
                time.sleep(1)

    def _parse_log_line(self, line: str) -> tuple[DateTime | None, str]:
        """
        Parse K8s log line and returns the final state.

        :param line: k8s log line
        :return: timestamp and log message
        """
        timestamp, sep, message = line.strip().partition(" ")
        if not sep:
            return None, line
        try:
            last_log_time = cast(DateTime, pendulum.parse(timestamp))
        except ParserError:
            return None, line
        return last_log_time, message

    async def _container_is_running_async(
        self, pod_name: str, namespace: str, container_name: str
    ) -> bool:
        """Read pod and checks if container is running."""
        remote_pod = await self.read_pod(pod_name, namespace)
        return container_is_running(pod=remote_pod, container_name=container_name)

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True,
    )
    async def _read_pod_logs(
        self,
        pod_name: str,
        namespace: str,
        container_name: str,
        tail_lines: int | None = None,
        timestamps: bool = False,
        since_seconds: int | None = None,
        follow=True,
        post_termination_timeout: int = 120,
    ) -> PodLogsConsumerAsync:
        """Read log from the POD."""
        additional_kwargs = {}
        if since_seconds:
            additional_kwargs["since_seconds"] = since_seconds

        if tail_lines:
            additional_kwargs["tail_lines"] = tail_lines

        try:
            logs = await self._k8s_client.read_namespaced_pod_log(
                name=pod_name,
                namespace=namespace,
                container=container_name,
                follow=follow,
                timestamps=timestamps,
                _preload_content=False,
                **additional_kwargs,
            )
        except BaseHTTPError:
            self.log.exception("There was an error reading the kubernetes API.")
            raise

        return PodLogsConsumerAsync(
            response=logs,
            pod_name=pod_name,
            namespace=namespace,
            read_pod_async=self.read_pod,
            container_name=container_name,
            post_termination_timeout=post_termination_timeout,
        )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True,
    )
    async def read_pod(self, pod_name: str, namespace: str) -> V1Pod:
        """Read POD information."""
        try:
            return await self._k8s_client.read_namespaced_pod(pod_name, namespace)
        except BaseHTTPError as e:
            raise AirflowException(
                f"There was an error reading the kubernetes API: {e}"
            )


class PodLogManager(LoggingMixin):
    """Monitor logs of Kubernetes pods."""

    def __init__(
        self, k8s_context: str, token_retriever: Optional[TokenRetriever] = None
    ):
        """
        Create the launcher.

        :param k8s_context: kubernetes context
        :param token_retriever: Retrieves auth tokens
        """
        super().__init__()
        self._k8s_context = k8s_context
        self._watch = watch.Watch()
        self._token_retriever = token_retriever

    def _refresh_k8s_auth_token(self):
        if self._token_retriever is not None:
            self._k8s_client.api_client.configuration.api_key["BearerToken"] = (
                f"Bearer {self._token_retriever.get_token()}"
            )

    @cached_property
    def _k8s_client(self) -> client:
        config.load_kube_config(context=self._k8s_context)
        return client.CoreV1Api()

    def fetch_container_logs(
        self,
        pod_name: str,
        namespace: str,
        container_name: str,
        *,
        follow=False,
        since_time: DateTime | None = None,
        post_termination_timeout: int = 120,
    ) -> PodLoggingStatus:
        """
        Follow the logs of container and stream to airflow logging.

        Returns when container exits.

        Between when the pod starts and logs being available, there might be a delay due to CSR not approved
        and signed yet. In such situation, ApiException is thrown. This is why we are retrying on this
        specific exception.
        """

        @tenacity.retry(
            retry=tenacity.retry_if_exception_type(ApiException),
            stop=tenacity.stop_after_attempt(10),
            wait=tenacity.wait_fixed(1),
        )
        def consume_logs(
            *,
            since_time: DateTime | None = None,
            follow: bool = True,
            logs: PodLogsConsumer | None,
        ) -> tuple[DateTime | None, PodLogsConsumer | None]:
            """
            Try to follow container logs until container completes.

            For a long-running container, sometimes the log read may be interrupted
            Such errors of this kind are suppressed.

            Returns the last timestamp observed in logs.
            """
            last_captured_timestamp = None
            try:
                logs = self._read_pod_logs(
                    pod_name=pod_name,
                    namespace=namespace,
                    container_name=container_name,
                    timestamps=True,
                    since_seconds=(
                        math.ceil((pendulum.now() - since_time).total_seconds())
                        if since_time
                        else None
                    ),
                    follow=follow,
                    post_termination_timeout=post_termination_timeout,
                )
                message_to_log = None
                message_timestamp = None
                progress_callback_lines = []
                try:
                    for raw_line in logs:
                        line = raw_line.decode("utf-8", errors="backslashreplace")
                        line_timestamp, message = self._parse_log_line(line)
                        if line_timestamp:  # detect new log line
                            if message_to_log is None:  # first line in the log
                                message_to_log = message
                                message_timestamp = line_timestamp
                                progress_callback_lines.append(line)
                            else:  # previous log line is complete
                                self.log.info("[%s] %s", container_name, message_to_log)
                                last_captured_timestamp = message_timestamp
                                message_to_log = message
                                message_timestamp = line_timestamp
                                progress_callback_lines = [line]
                        else:  # continuation of the previous log line
                            message_to_log = f"{message_to_log}\n{message}"
                            progress_callback_lines.append(line)
                finally:
                    if message_to_log is not None:
                        self.log.info("[%s] %s", container_name, message_to_log)
                        last_captured_timestamp = message_timestamp
            except BaseHTTPError as e:
                self.log.warning(
                    "Reading of logs interrupted for container %r with error %r; will retry. "
                    "Set log level to DEBUG for traceback.",
                    container_name,
                    e,
                )
                self.log.debug(
                    "Traceback for interrupted logs read for pod %r",
                    pod_name,
                    exc_info=True,
                )
            return last_captured_timestamp or since_time, logs

        # note: `read_pod_logs` follows the logs, so we shouldn't necessarily *need* to
        # loop as we do here. But in a long-running process we might temporarily lose connectivity.
        # So the looping logic is there to let us resume following the logs.
        logs = None
        last_log_time = since_time
        while True:
            last_log_time, logs = consume_logs(
                since_time=last_log_time,
                follow=follow,
                logs=logs,
            )
            if not self._container_is_running(
                pod_name, namespace, container_name=container_name
            ):
                return PodLoggingStatus(running=False, last_log_time=last_log_time)
            if not follow:
                return PodLoggingStatus(running=True, last_log_time=last_log_time)
            else:
                self.log.warning(
                    "Pod %s log read interrupted but container %s still running",
                    pod_name,
                    container_name,
                )
                time.sleep(1)
            self._refresh_k8s_auth_token()

    def _parse_log_line(self, line: str) -> tuple[DateTime | None, str]:
        """
        Parse K8s log line and returns the final state.

        :param line: k8s log line
        :return: timestamp and log message
        """
        timestamp, sep, message = line.strip().partition(" ")
        if not sep:
            return None, line
        try:
            last_log_time = cast(DateTime, pendulum.parse(timestamp))
        except ParserError:
            return None, line
        return last_log_time, message

    def _container_is_running(
        self, pod_name: str, namespace: str, container_name: str
    ) -> bool:
        """Read pod and checks if container is running."""
        remote_pod = self.read_pod(pod_name, namespace)
        return container_is_running(pod=remote_pod, container_name=container_name)

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True,
    )
    def _read_pod_logs(
        self,
        pod_name: str,
        namespace: str,
        container_name: str,
        tail_lines: int | None = None,
        timestamps: bool = False,
        since_seconds: int | None = None,
        follow=True,
        post_termination_timeout: int = 120,
    ) -> PodLogsConsumer:
        """Read log from the POD."""
        additional_kwargs = {}
        if since_seconds:
            additional_kwargs["since_seconds"] = since_seconds

        if tail_lines:
            additional_kwargs["tail_lines"] = tail_lines

        try:
            logs = self._k8s_client.read_namespaced_pod_log(
                name=pod_name,
                namespace=namespace,
                container=container_name,
                follow=follow,
                timestamps=timestamps,
                _preload_content=False,
                **additional_kwargs,
            )
        except BaseHTTPError:
            self.log.exception("There was an error reading the kubernetes API.")
            raise

        return PodLogsConsumer(
            response=logs,
            pod_name=pod_name,
            namespace=namespace,
            read_pod=self.read_pod,
            container_name=container_name,
            post_termination_timeout=post_termination_timeout,
        )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True,
    )
    def read_pod(self, pod_name: str, namespace: str) -> V1Pod:
        """Read POD information."""
        try:
            return self._k8s_client.read_namespaced_pod(pod_name, namespace)
        except BaseHTTPError as e:
            raise AirflowException(
                f"There was an error reading the kubernetes API: {e}"
            )
