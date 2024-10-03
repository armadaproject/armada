import dataclasses
from functools import cached_property
from typing import Optional

import grpc
from airflow.models import TaskInstance
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from armada.model import GrpcChannelArgs
from armada_client.armada.job_pb2 import JobRunDetails
from armada_client.armada.submit_pb2 import JobSubmitRequestItem
from armada_client.client import ArmadaClient
from armada_client.typings import JobState
from pendulum import DateTime
import tenacity

from .utils import log_exceptions
from .model import RunningJobContext

class ArmadaHook(LoggingMixin):
    def __init__(self, args: GrpcChannelArgs):
        self.args = args

    @cached_property
    def client(self):
        return ArmadaClient(channel=self._create_channel())

    def _create_channel(self) -> grpc.Channel:
        if self.args.auth is None:
            return grpc.insecure_channel(
                target=self.args.target, options=self.args.options, compression=self.args.compression
            )

        return grpc.secure_channel(
            target=self.args.target,
            options=self.args.options,
            compression=self.args.compression,
            credentials=grpc.composite_channel_credentials(
                grpc.ssl_channel_credentials(),
                grpc.metadata_call_credentials(self.args.auth),
            ),
        )

    @tenacity.retry(
        wait=tenacity.wait_random_exponential(max=15),
        reraise=True,
    )
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

    @tenacity.retry(
        wait=tenacity.wait_random_exponential(max=15),
        reraise=True,
    )
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

    @tenacity.retry(
        wait=tenacity.wait_random_exponential(max=15),
        reraise=True,
    )
    def refresh_context(
        self, job_context: RunningJobContext, tracking_url: str
    ) -> RunningJobContext:
        response = self.client.get_job_status([job_context.job_id])
        state = JobState(response.job_states[job_context.job_id])
        if state != job_context.state and tracking_url:
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
    
    @tenacity.retry(
        wait=tenacity.wait_random_exponential(max=15),
        reraise=True,
    )
    @log_exceptions
    def context_from_xcom(self, ti: TaskInstance, re_attach: bool) -> RunningJobContext:
        result = ti.xcom_pull(key="job_context")
        if result:
            return self.refresh_context(RunningJobContext(
                armada_queue=result["armada_queue"],
                job_id=result["armada_job_id"],
                job_set_id=result["armada_job_set_id"],
                submit_time=DateTime.utcnow() if re_attach else result.get("armada_job_submit_time", DateTime.utcnow()),
                last_log_time=None if re_attach else result.get("armada_job_last_log_time", None)
            ), None)

        return None

    @tenacity.retry(
        wait=tenacity.wait_random_exponential(max=15),
        reraise=True,
    )
    def context_to_xcom(self, ti: TaskInstance, ctx: RunningJobContext, lookout_url: str):
        ti.xcom_push(key="job_context", value={
                "armada_queue": ctx.armada_queue,
                "armada_job_id": ctx.job_id,
                "armada_job_set_id": ctx.job_set_id,
                "armada_job_submit_time": ctx.submit_time,
                "armada_job_last_log_time": ctx.last_log_time,
                "armada_lookout_url": lookout_url,
        })


    def _get_latest_job_run_details(self, job_id) -> Optional[JobRunDetails]:
        job_details = self.client.get_job_details([job_id]).job_details[job_id]
        if job_details and job_details.latest_run_id:
            for run in job_details.job_runs:
                if run.run_id == job_details.latest_run_id:
                    return run
        return None
