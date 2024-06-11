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

import queue
from datetime import timedelta
from http.client import HTTPResponse
from typing import Generator, TYPE_CHECKING, Callable, Awaitable

from aiohttp.client_exceptions import ClientResponse
from airflow.utils.timezone import utcnow
from kubernetes.client import V1Pod
from kubernetes_asyncio.client import V1Pod as aio_V1Pod

from armada.logs.utils import container_is_running, get_container_status


if TYPE_CHECKING:
    from urllib3.response import HTTPResponse


class PodLogsConsumerAsync:
    """
    Responsible for pulling pod logs from a stream asynchronously, checking the container status before reading data.

    This class contains a workaround for the issue https://github.com/apache/airflow/issues/23497.

    :param response: HTTP response with logs
    :param pod: Pod instance from Kubernetes client
    :param read_pod_async: Callable returning a pod object that can be awaited on,
        given (pod name, namespace) as arguments
    :param container_name: Name of the container that we're reading logs from
    :param post_termination_timeout: (Optional) The period of time in seconds representing for how long time
        logs are available after the container termination.
    :param read_pod_cache_timeout: (Optional) The container's status cache lifetime.
        The container status is cached to reduce API calls.

    :meta private:
    """

    def __init__(
        self,
        response: ClientResponse,
        pod_name: str,
        namespace: str,
        read_pod_async: Callable[[str, str], Awaitable[aio_V1Pod]],
        container_name: str,
        post_termination_timeout: int = 120,
        read_pod_cache_timeout: int = 120,
    ):
        self.response = response
        self.pod_name = pod_name
        self.namespace = namespace
        self._read_pod_async = read_pod_async
        self.container_name = container_name
        self.post_termination_timeout = post_termination_timeout
        self.last_read_pod_at = None
        self.read_pod_cache = None
        self.read_pod_cache_timeout = read_pod_cache_timeout
        self.log_queue = queue.Queue()

    def __aiter__(self):
        return self

    async def __anext__(self):
        r"""Yield log items divided by the '\n' symbol."""
        if not self.log_queue.empty():
            return self.log_queue.get()

        incomplete_log_item: list[bytes] = []
        if await self.logs_available():
            async for data_chunk in self.response.content:
                if b"\n" in data_chunk:
                    log_items = data_chunk.split(b"\n")
                    for x in self._extract_log_items(incomplete_log_item, log_items):
                        if x is not None:
                            self.log_queue.put(x)
                    incomplete_log_item = self._save_incomplete_log_item(log_items[-1])
                else:
                    incomplete_log_item.append(data_chunk)
                if not await self.logs_available():
                    break
        else:
            self.response.close()
            raise StopAsyncIteration
        if incomplete_log_item:
            item = b"".join(incomplete_log_item)
            if item is not None:
                self.log_queue.put(item)

        # Prevents method from returning None
        if not self.log_queue.empty():
            return self.log_queue.get()

        self.response.close()
        raise StopAsyncIteration

    @staticmethod
    def _extract_log_items(incomplete_log_item: list[bytes], log_items: list[bytes]):
        yield b"".join(incomplete_log_item) + log_items[0] + b"\n"
        for x in log_items[1:-1]:
            yield x + b"\n"

    @staticmethod
    def _save_incomplete_log_item(sub_chunk: bytes):
        return [sub_chunk] if [sub_chunk] else []

    async def logs_available(self):
        remote_pod = await self.read_pod()
        if container_is_running(pod=remote_pod, container_name=self.container_name):
            return True
        container_status = get_container_status(
            pod=remote_pod, container_name=self.container_name
        )
        state = container_status.state if container_status else None
        terminated = state.terminated if state else None
        if terminated:
            termination_time = terminated.finished_at
            if termination_time:
                return (
                    termination_time + timedelta(seconds=self.post_termination_timeout)
                    > utcnow()
                )
        return False

    async def read_pod(self):
        _now = utcnow()
        if (
            self.read_pod_cache is None
            or self.last_read_pod_at + timedelta(seconds=self.read_pod_cache_timeout)
            < _now
        ):
            self.read_pod_cache = await self._read_pod_async(
                self.pod_name, self.namespace
            )
            self.last_read_pod_at = _now
        return self.read_pod_cache


class PodLogsConsumer:
    """
    Responsible for pulling pod logs from a stream with checking a container status before reading data.

    This class is a workaround for the issue https://github.com/apache/airflow/issues/23497.

    :param response: HTTP response with logs
    :param pod: Pod instance from Kubernetes client
    :param read_pod: Callable returning a pod object given (pod name, namespace) as arguments
    :param container_name: Name of the container that we're reading logs from
    :param post_termination_timeout: (Optional) The period of time in seconds representing for how long time
        logs are available after the container termination.
    :param read_pod_cache_timeout: (Optional) The container's status cache lifetime.
        The container status is cached to reduce API calls.

    :meta private:
    """

    def __init__(
        self,
        response: HTTPResponse,
        pod_name: str,
        namespace: str,
        read_pod: Callable[[str, str], V1Pod],
        container_name: str,
        post_termination_timeout: int = 120,
        read_pod_cache_timeout: int = 120,
    ):
        self.response = response
        self.pod_name = pod_name
        self.namespace = namespace
        self._read_pod = read_pod
        self.container_name = container_name
        self.post_termination_timeout = post_termination_timeout
        self.last_read_pod_at = None
        self.read_pod_cache = None
        self.read_pod_cache_timeout = read_pod_cache_timeout

    def __iter__(self) -> Generator[bytes, None, None]:
        r"""Yield log items divided by the '\n' symbol."""
        incomplete_log_item: list[bytes] = []
        if self.logs_available():
            for data_chunk in self.response.stream(amt=None, decode_content=True):
                if b"\n" in data_chunk:
                    log_items = data_chunk.split(b"\n")
                    yield from self._extract_log_items(incomplete_log_item, log_items)
                    incomplete_log_item = self._save_incomplete_log_item(log_items[-1])
                else:
                    incomplete_log_item.append(data_chunk)
                if not self.logs_available():
                    break
        if incomplete_log_item:
            yield b"".join(incomplete_log_item)

    @staticmethod
    def _extract_log_items(incomplete_log_item: list[bytes], log_items: list[bytes]):
        yield b"".join(incomplete_log_item) + log_items[0] + b"\n"
        for x in log_items[1:-1]:
            yield x + b"\n"

    @staticmethod
    def _save_incomplete_log_item(sub_chunk: bytes):
        return [sub_chunk] if [sub_chunk] else []

    def logs_available(self):
        remote_pod = self.read_pod()
        if container_is_running(pod=remote_pod, container_name=self.container_name):
            return True
        container_status = get_container_status(
            pod=remote_pod, container_name=self.container_name
        )
        state = container_status.state if container_status else None
        terminated = state.terminated if state else None
        if terminated:
            termination_time = terminated.finished_at
            if termination_time:
                return (
                    termination_time + timedelta(seconds=self.post_termination_timeout)
                    > utcnow()
                )
        return False

    def read_pod(self):
        _now = utcnow()
        if (
            self.read_pod_cache is None
            or self.last_read_pod_at + timedelta(seconds=self.read_pod_cache_timeout)
            < _now
        ):
            self.read_pod_cache = self._read_pod(self.pod_name, self.namespace)
            self.last_read_pod_at = _now
        return self.read_pod_cache
