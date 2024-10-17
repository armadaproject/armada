from __future__ import annotations

import math
from http.client import HTTPResponse
from typing import List, Optional, Tuple, cast

import pendulum
import tenacity
from airflow.utils.log.logging_mixin import LoggingMixin
from armada.auth import TokenRetriever
from kubernetes import client, config
from pendulum import DateTime
from pendulum.parsing.exceptions import ParserError
from urllib3.exceptions import HTTPError


class KubernetesPodLogManager(LoggingMixin):
    """Monitor logs of Kubernetes pods asynchronously."""

    def __init__(
        self,
        token_retriever: Optional[TokenRetriever] = None,
    ):
        """
        Create PodLogManger.
        :param token_retriever: Retrieves auth tokens
        """
        super().__init__()
        self._token_retriever = token_retriever

    def _k8s_client(self, k8s_context) -> client.CoreV1Api:
        configuration = client.Configuration()
        config.load_kube_config(client_configuration=configuration, context=k8s_context)
        k8s_client = client.CoreV1Api(
            api_client=client.ApiClient(configuration=configuration)
        )
        k8s_client.api_client.configuration.api_key["authorization"] = (
            f"Bearer {self._token_retriever.get_token()}"
        )

        return k8s_client

    @tenacity.retry(
        wait=tenacity.wait_exponential(max=3),
        retry=tenacity.retry_if_exception_type(HTTPError),
        stop=tenacity.stop_after_attempt(5),
        reraise=True,
    )
    def fetch_container_logs(
        self,
        *,
        k8s_context: str,
        namespace: str,
        pod: str,
        container: str,
        since_time: Optional[DateTime],
    ) -> Optional[DateTime]:
        """
        Fetches container logs, do not follow container logs.
        """
        client = self._k8s_client(k8s_context)
        since_seconds = (
            math.ceil((pendulum.now() - since_time).total_seconds())
            if since_time
            else None
        )
        try:
            logs = client.read_namespaced_pod_log(
                namespace=namespace,
                name=pod,
                container=container,
                follow=False,
                timestamps=True,
                since_seconds=since_seconds,
                _preload_content=False,
            )
            if logs.status == 404:
                self.log.warning(f"Unable to fetch logs - pod {pod} has been deleted.")
                return since_time
        except HTTPError as e:
            self.log.exception(f"There was an error reading the kubernetes API: {e}.")
            raise

        return self._stream_logs(container, since_time, logs)

    def _stream_logs(
        self, container: str, since_time: Optional[DateTime], logs: HTTPResponse
    ) -> Optional[DateTime]:
        messages: List[str] = []
        message_timestamp = None
        try:
            chunk = logs.read()
            lines = chunk.decode("utf-8", errors="backslashreplace").splitlines()
            for raw_line in lines:
                line_timestamp, message = self._parse_log_line(raw_line)

                if line_timestamp:  # detect new log-line (starts with timestamp)
                    if since_time and line_timestamp <= since_time:
                        continue
                    self._log_container_message(container, messages)
                    messages.clear()
                    message_timestamp = line_timestamp
                messages.append(message)
        except HTTPError as e:
            self.log.warning(
                f"Reading of logs interrupted for container {container} with error {e}."
            )

        self._log_container_message(container, messages)
        return message_timestamp

    def _log_container_message(self, container: str, messages: List[str]):
        if messages:
            self.log.info("[%s] %s", container, "\n".join(messages))

    def _parse_log_line(self, line: bytes) -> Tuple[DateTime | None, str]:
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
