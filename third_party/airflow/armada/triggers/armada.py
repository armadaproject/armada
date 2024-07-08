import asyncio
import importlib
import time
from functools import cached_property
from typing import AsyncIterator, Any, Optional, Tuple, Dict

from airflow.triggers.base import BaseTrigger, TriggerEvent
from armada_client.armada.job_pb2 import JobRunDetails
from armada_client.typings import JobState

from armada_client.asyncio_client import ArmadaAsyncIOClient
from armada.auth import TokenRetriever
from armada.logs.pod_log_manager import PodLogManagerAsync
from armada.model import GrpcChannelArgs
from pendulum import DateTime


class ArmadaTrigger(BaseTrigger):
    """
    An Airflow Trigger that can asynchronously manage an Armada job.
    """

    def __init__(
        self,
        job_id: str,
        armada_queue: str,
        job_set_id: str,
        poll_interval: int,
        tracking_message: str,
        job_acknowledgement_timeout: int,
        job_request_namespace: str,
        channel_args: GrpcChannelArgs = None,
        channel_args_details: Dict[str, Any] = None,
        container_logs: Optional[str] = None,
        k8s_token_retriever: Optional[TokenRetriever] = None,
        k8s_token_retriever_details: Optional[Tuple[str, Dict[str, Any]]] = None,
        last_log_time: Optional[DateTime] = None,
    ):
        """
        Initializes an instance of ArmadaTrigger, which is an Airflow trigger for
        managing Armada jobs asynchronously.

        :param job_id: The unique identifier of the job to be monitored.
        :type job_id: str
        :param armada_queue: The Armada queue under which the job was submitted.
        Required for job cancellation.
        :type armada_queue: str
        :param job_set_id: The unique identifier of the job set under which the job
        was submitted. Required for job cancellation.
        :type job_set_id: str
        :param poll_interval: The interval, in seconds, at which the job status will be
        checked.
        :type poll_interval: int
        :param tracking_message: A message to log or display for tracking the job
         status.
        :type tracking_message: str
        :param job_acknowledgement_timeout: The timeout, in seconds, to wait for the job
        to be acknowledged by Armada.
        :type job_acknowledgement_timeout: int
        :param job_request_namespace: The Kubernetes namespace under which the job was
        submitted.
        :type job_request_namespace: str
        :param channel_args: The arguments to configure the gRPC channel. If None,
        default arguments will be used.
        :type channel_args: GrpcChannelArgs, optional
        :param channel_args_details: Additional details or configurations for the gRPC
        channel as a dictionary. Only used when
        the trigger is rehydrated after serialization.
        :type channel_args_details: dict[str, Any], optional
        :param container_logs: Name of container from which to retrieve logs
        :type container_logs: str, optional
        :param k8s_token_retriever: An optional instance of type TokenRetriever, used to
        refresh the Kubernetes auth token
        :type k8s_token_retriever: TokenRetriever, optional
        :param k8s_token_retriever_details: Configuration for TokenRetriever as a
         dictionary.
        Only used when the trigger is
        rehydrated after serialization.
        :type k8s_token_retriever_details: Tuple[str, Dict[str, Any]], optional
        :param last_log_time: where to resume logs from
        :type last_log_time: DateTime, optional
        """
        super().__init__()
        self.job_id = job_id
        self.armada_queue = armada_queue
        self.job_set_id = job_set_id
        self.poll_interval = poll_interval
        self.tracking_message = tracking_message
        self.job_acknowledgement_timeout = job_acknowledgement_timeout
        self.container_logs = container_logs
        self.last_log_time = last_log_time
        self.job_request_namespace = job_request_namespace
        self._pod_manager = None
        self.k8s_token_retriever = k8s_token_retriever

        if channel_args:
            self.channel_args = channel_args
        elif channel_args_details:
            self.channel_args = GrpcChannelArgs(**channel_args_details)
        else:
            raise f"must provide either {channel_args} or {channel_args_details}"

        if k8s_token_retriever_details:
            classpath, kwargs = k8s_token_retriever_details
            module_path, class_name = classpath.rsplit(
                ".", 1
            )  # Split the classpath to module and class name
            module = importlib.import_module(
                module_path
            )  # Dynamically import the module
            cls = getattr(module, class_name)  # Get the class from the module
            self.k8s_token_retriever = cls(
                **kwargs
            )  # Instantiate the class with the deserialized kwargs

    def serialize(self) -> tuple:
        """
        Serialises the state of this Trigger.
        When the Trigger is re-hydrated, these values will be passed to init() as kwargs
        :return:
        """
        k8s_token_retriever_details = (
            self.k8s_token_retriever.serialize() if self.k8s_token_retriever else None
        )
        return (
            "armada.triggers.armada.ArmadaTrigger",
            {
                "job_id": self.job_id,
                "armada_queue": self.armada_queue,
                "job_set_id": self.job_set_id,
                "channel_args_details": self.channel_args.serialize(),
                "poll_interval": self.poll_interval,
                "tracking_message": self.tracking_message,
                "job_acknowledgement_timeout": self.job_acknowledgement_timeout,
                "container_logs": self.container_logs,
                "k8s_token_retriever_details": k8s_token_retriever_details,
                "last_log_time": self.last_log_time,
                "job_request_namespace": self.job_request_namespace,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Run the Trigger Asynchronously. This will poll Armada until the Job reaches a
        terminal state
        """
        try:
            response = await self._poll_for_termination(self.job_id)
            yield TriggerEvent(response)
        except Exception as exc:
            yield TriggerEvent(
                {
                    "status": "error",
                    "job_id": self.job_id,
                    "response": f"Job {self.job_id} did not succeed. Error was {exc}",
                }
            )

    """Cannot call on_kill from trigger, will asynchronously cancel jobs instead."""

    async def _cancel_job(self) -> None:
        try:
            result = await self.client.cancel_jobs(
                queue=self.armada_queue,
                job_set_id=self.job_set_id,
                job_id=self.job_id,
            )
            if len(list(result.cancelled_ids)) > 0:
                self.log.info(f"Cancelled job with id {result.cancelled_ids}")
            else:
                self.log.warning(f"Failed to cancel job with id {self.job_id}")
        except Exception as e:
            self.log.warning(f"Failed to cancel job with id {self.job_id}: {e}")

    async def _poll_for_termination(self, job_id: str) -> Dict[str, Any]:
        state = JobState.UNKNOWN
        start_time = time.time()
        job_acknowledged = False
        run_details = None

        # Poll for terminal state
        while state.is_active():
            resp = await self.client.get_job_status([job_id])
            state = JobState(resp.job_states[job_id])
            self.log.info(
                f"Job {job_id} is in state: {state.name}. {self.tracking_message}"
            )

            if state != JobState.UNKNOWN:
                job_acknowledged = True

            if (
                not job_acknowledged
                and int(time.time() - start_time) > self.job_acknowledgement_timeout
            ):
                await self._cancel_job()
                return {
                    "status": "error",
                    "job_id": job_id,
                    "response": f"Job {job_id} not acknowledged within timeout "
                    f"{self.job_acknowledgement_timeout}.",
                }

            if self.container_logs and not run_details:
                if state == JobState.RUNNING or state.is_terminal():
                    run_details = await self._get_latest_job_run_details(self.job_id)

            if run_details:
                try:
                    log_status = await self.pod_manager(
                        run_details.cluster
                    ).fetch_container_logs(
                        pod_name=f"armada-{self.job_id}-0",
                        namespace=self.job_request_namespace,
                        container_name=self.container_logs,
                        since_time=self.last_log_time,
                    )
                    self.last_log_time = log_status.last_log_time
                except Exception as e:
                    self.log.exception(e)

            if state.is_active():
                self.log.debug(f"Sleeping for {self.poll_interval} seconds")
                await asyncio.sleep(self.poll_interval)

        self.log.info(f"Job {job_id} terminated with state:{state.name}")
        if state != JobState.SUCCEEDED:
            return {
                "status": "error",
                "job_id": job_id,
                "response": f"Job {job_id} did not succeed. Final status was "
                f"{state.name}",
            }
        return {
            "status": "success",
            "job_id": job_id,
            "response": f"Job {job_id} succeeded",
        }

    @cached_property
    def client(self) -> ArmadaAsyncIOClient:
        return ArmadaAsyncIOClient(channel=self.channel_args.aio_channel())

    def pod_manager(self, k8s_context: str) -> PodLogManagerAsync:
        if self._pod_manager is None:
            self._pod_manager = PodLogManagerAsync(
                k8s_context=k8s_context, token_retriever=self.k8s_token_retriever
            )

        return self._pod_manager

    async def _get_latest_job_run_details(self, job_id) -> Optional[JobRunDetails]:
        resp = await self.client.get_job_details([job_id])
        job_details = resp.job_details[job_id]
        if job_details and job_details.latest_run_id:
            for run in job_details.job_runs:
                if run.run_id == job_details.latest_run_id:
                    return run
        return None

    def __eq__(self, other):
        if not isinstance(other, ArmadaTrigger):
            return False
        return (
            self.job_id == other.job_id
            and self.channel_args.serialize() == other.channel_args.serialize()
            and self.poll_interval == other.poll_interval
            and self.tracking_message == other.tracking_message
        )
