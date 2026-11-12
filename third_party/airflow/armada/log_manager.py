from __future__ import annotations

import math
from http.client import HTTPResponse
from typing import List, Optional, Tuple, cast

import pendulum
import tenacity
from airflow.utils.log.logging_mixin import LoggingMixin
from kubernetes import client, config
from pendulum import DateTime
from pendulum.parsing.exceptions import ParserError
from urllib3.exceptions import HTTPError

from .auth import TokenRetriever
from .links import UrlFromLogsExtractor
from .model import LogPosition


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
        since: Optional[LogPosition],
        link_extractor: UrlFromLogsExtractor,
    ) -> Optional[LogPosition]:
        """
        Fetches container logs, do not follow container logs.
        """
        client = self._k8s_client(k8s_context)
        since_seconds = (
            math.ceil((pendulum.now() - since.time).total_seconds()) if since else None
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
                return since
        except HTTPError as e:
            self.log.exception(f"There was an error reading the kubernetes API: {e}.")
            raise

        return self._stream_logs(container, since, logs, link_extractor)

    def _stream_logs(
        self,
        container: str,
        since: Optional[LogPosition],
        logs: HTTPResponse,
        link_extractor: UrlFromLogsExtractor,
    ) -> Optional[LogPosition]:
        """
        Logs what the response holds beyond `since` and returns the new position.

        `since_seconds` has whole-second granularity, so every poll returns the last
        record again, possibly grown. Only its characters beyond the offset are new.
        """
        try:
            chunk = logs.read()
        except HTTPError as e:
            self.log.warning(
                f"Reading of logs interrupted for container {container} with error {e}."
            )
            return since

        position = since
        text = chunk.decode("utf-8", errors="backslashreplace")
        for timestamp, message in self._parse_messages(text):
            if timestamp is None:
                self._log_container_message(container, message, link_extractor)
                continue
            if since and timestamp < since.time:
                continue
            logged = since.offset if since and timestamp == since.time else 0
            new_text = message[logged:].lstrip("\r\n")
            self._log_container_message(container, new_text, link_extractor)
            position = LogPosition(timestamp, max(logged, len(message)))

        return position

    def _parse_messages(self, text: str) -> List[Tuple[DateTime | None, str]]:
        """
        Groups k8s log lines into one message per timestamp.

        Lines are split on newlines only: the kubelet stamps each newline-terminated
        record, and a carriage return inside one is part of that record.
        """
        messages: List[Tuple[DateTime | None, str]] = []
        for raw_line in text.split("\n"):
            if not raw_line:
                continue
            line_timestamp, line = self._parse_log_line(raw_line)
            if messages and line_timestamp in (None, messages[-1][0]):
                timestamp, message = messages[-1]
                messages[-1] = (timestamp, f"{message}\n{line}")
                continue
            messages.append((line_timestamp, line))
        return messages

    def _log_container_message(
        self, container: str, message: str, link_extractor: UrlFromLogsExtractor
    ):
        message = "\n".join(message.splitlines())
        if message:
            link_extractor.extract_and_persist_urls(message)
            self.log.info("[%s] %s", container, message)

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
