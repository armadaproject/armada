import dataclasses
import json
import threading
from functools import cached_property
from typing import Dict, Optional

import grpc
from airflow.exceptions import AirflowException
from airflow.serialization.serde import serialize
from airflow.utils.log.logging_mixin import LoggingMixin
from armada.model import GrpcChannelArgs
from armada_client.armada.job_pb2 import JobRunDetails
from armada_client.armada.submit_pb2 import JobSubmitRequestItem
from armada_client.client import ArmadaClient
from armada_client.typings import JobState
from pendulum import DateTime

from .model import RunningJobContext


class ArmadaClientFactory:
    CLIENTS_LOCK = threading.Lock()
    CLIENTS: Dict[str, ArmadaClient] = {}

    @staticmethod
    def client_for(args: GrpcChannelArgs) -> ArmadaClient:
        """
        Armada clients, maintain GRPC connection to Armada API.
        We cache them per channel args config in class level cache.

        Access to this method can be from multiple-threads.
        """
        channel_args_key = json.dumps(serialize(args))
        with ArmadaClientFactory.CLIENTS_LOCK:
            if channel_args_key not in ArmadaClientFactory.CLIENTS:
                ArmadaClientFactory.CLIENTS[channel_args_key] = ArmadaClient(
                    channel=ArmadaClientFactory._create_channel(args)
                )
            return ArmadaClientFactory.CLIENTS[channel_args_key]

    @staticmethod
    def _create_channel(args: GrpcChannelArgs) -> grpc.Channel:
        if args.auth is None:
            return grpc.insecure_channel(
                target=args.target, options=args.options, compression=args.compression
            )

        return grpc.secure_channel(
            target=args.target,
            options=args.options,
            compression=args.compression,
            credentials=grpc.composite_channel_credentials(
                grpc.ssl_channel_credentials(),
                grpc.metadata_call_credentials(args.auth),
            ),
        )


class ArmadaHook(LoggingMixin):
    def __init__(self, args: GrpcChannelArgs):
        self.args = args

    @cached_property
    def client(self):
        return ArmadaClientFactory.client_for(self.args)

    def cancel_job(self, job_context: RunningJobContext) -> RunningJobContext:
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
        finally:
            return dataclasses.replace(job_context, job_state=JobState.CANCELLED.name)

    def submit_job(
        self, queue: str, job_set_id: str, job_request: JobSubmitRequestItem
    ) -> RunningJobContext:
        resp = self.client.submit_jobs(queue, job_set_id, [job_request])
        num_responses = len(resp.job_response_items)

        # We submitted exactly one job to armada, so we expect a single response
        if num_responses != 1:
            raise AirflowException(
                f"No valid received from Armada (expected 1 job to be created "
                f"but got {num_responses})"
            )
        job = resp.job_response_items[0]

        # Throw if armada told us we had submitted something bad
        if job.error:
            raise AirflowException(f"Error submitting job to Armada: {job.error}")

        return RunningJobContext(queue, job.job_id, job_set_id, DateTime.utcnow())

    def refresh_context(
        self, job_context: RunningJobContext, tracking_url: str
    ) -> RunningJobContext:
        response = self.client.get_job_status([job_context.job_id])
        state = JobState(response.job_states[job_context.job_id])
        if state != job_context.state:
            self.log.info(
                f"job {job_context.job_id} is in state: {state.name}. "
                f"{tracking_url}"
            )

        cluster = job_context.cluster
        if not cluster:
            # Job is running / or completed already
            if state == JobState.RUNNING or state.is_terminal():
                run_details = self._get_latest_job_run_details(job_context.job_id)
                if run_details:
                    cluster = run_details.cluster
        return dataclasses.replace(job_context, job_state=state.name, cluster=cluster)

    def _get_latest_job_run_details(self, job_id) -> Optional[JobRunDetails]:
        job_details = self.client.get_job_details([job_id]).job_details[job_id]
        if job_details and job_details.latest_run_id:
            for run in job_details.job_runs:
                if run.run_id == job_details.latest_run_id:
                    return run
        return None
