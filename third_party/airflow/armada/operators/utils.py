import asyncio
import logging
import os
import time

from airflow.exceptions import AirflowFailException
from typing import List, Optional, Tuple
from enum import Enum

from armada.operators.jobservice import JobServiceClient
from armada.operators.jobservice_asyncio import JobServiceAsyncIOClient
from armada.jobservice import jobservice_pb2
from armada_client.armada import submit_pb2


class JobState(Enum):
    SUBMITTED = 0
    DUPLICATE_FOUND = 1
    RUNNING = 2
    FAILED = 3
    SUCCEEDED = 4
    CANCELLED = 5
    JOB_ID_NOT_FOUND = 6
    CONNECTION_ERR = 7


_pb_to_job_state = {
    jobservice_pb2.JobServiceResponse.SUBMITTED: JobState.SUBMITTED,
    jobservice_pb2.JobServiceResponse.DUPLICATE_FOUND: JobState.DUPLICATE_FOUND,
    jobservice_pb2.JobServiceResponse.RUNNING: JobState.RUNNING,
    jobservice_pb2.JobServiceResponse.FAILED: JobState.FAILED,
    jobservice_pb2.JobServiceResponse.SUCCEEDED: JobState.SUCCEEDED,
    jobservice_pb2.JobServiceResponse.CANCELLED: JobState.CANCELLED,
    jobservice_pb2.JobServiceResponse.JOB_ID_NOT_FOUND: JobState.JOB_ID_NOT_FOUND,
    # NOTE(Clif): For whatever reason CONNECTION_ERR is not present in the
    # generated protobuf.
    7: JobState.CONNECTION_ERR,
}


def job_state_from_pb(state) -> JobState:
    return _pb_to_job_state[state]


def airflow_error(job_state: JobState, name: str, job_id: str):
    """Throw an error on a terminal event if job errored out

    :param job_state: A JobState enum class
    :param name: The name of your armada job
    :param job_id: The job id that armada assigns to it
    :return: No Return or an AirflowFailException.

    AirflowFailException tells Airflow Schedule to not reschedule the task

    """
    if job_state == JobState.SUCCEEDED:
        return
    if (
        job_state == JobState.FAILED
        or job_state == JobState.CANCELLED
        or job_state == JobState.JOB_ID_NOT_FOUND
    ):
        job_message = job_state.name
        raise AirflowFailException(f"The Armada job {name}:{job_id} {job_message}")


def default_job_status_callable(
    armada_queue: str,
    job_set_id: str,
    job_id: str,
    job_service_client: JobServiceClient,
) -> jobservice_pb2.JobServiceResponse:
    return job_service_client.get_job_status(
        queue=armada_queue, job_id=job_id, job_set_id=job_set_id
    )


armada_logger = logging.getLogger("airflow.task")


def search_for_job_complete(
    armada_queue: str,
    job_set_id: str,
    airflow_task_name: str,
    job_id: str,
    job_service_client: Optional[JobServiceClient] = None,
    job_status_callable=default_job_status_callable,
    time_out_for_failure: int = 7200,
) -> Tuple[JobState, str]:
    """

    Poll JobService cache until you get a terminated event.

    A terminated event is SUCCEEDED, FAILED or CANCELLED

    :param armada_queue: The queue for armada
    :param job_set_id: Your job_set_id
    :param airflow_task_name: The name of your armada job
    :param job_id: The name of the job id that armada assigns to it
    :param job_service_client: A JobServiceClient that is used for polling.
                                It is optional only for testing
    :param job_status_callable: A callable object for test injection.
    :param time_out_for_failure: The amount of time a job
                                    can be in job_id_not_found
                                    before we decide it was a invalid job
    :return: A tuple of JobStateEnum, message
    """
    start_time = time.time()
    # Overwrite time_out_for_failure by environment variable for configuration
    armada_time_out_env = os.getenv("ARMADA_AIRFLOW_TIME_OUT_JOB_ID")
    if armada_time_out_env:
        time_out_for_failure = int(armada_time_out_env)
    while True:
        # The else statement is for testing purposes.
        # We want to allow a test callable to be passed
        if job_service_client:
            job_status_return = job_status_callable(
                armada_queue=armada_queue,
                job_id=job_id,
                job_set_id=job_set_id,
                job_service_client=job_service_client,
            )
        else:
            job_status_return = job_status_callable(
                armada_queue=armada_queue, job_id=job_id, job_set_id=job_set_id
            )

        job_state = job_state_from_pb(job_status_return.state)
        armada_logger.debug(f"Got job state '{job_state.name}' for job {job_id}")

        time.sleep(3)
        if job_state == JobState.SUCCEEDED:
            job_message = f"Armada {airflow_task_name}:{job_id} succeeded"
            break
        if job_state == JobState.FAILED:
            job_message = (
                f"Armada {airflow_task_name}:{job_id} failed\n"
                f"failed with reason {job_status_return.error}"
            )
            break
        if job_state == JobState.CANCELLED:
            job_message = f"Armada {airflow_task_name}:{job_id} cancelled"
            break
        if job_state == JobState.CONNECTION_ERR:
            log_messages = (
                f"Armada {airflow_task_name}:{job_id} connection error (will retry)"
                f"failed with reason {job_status_return.error}"
            )
            armada_logger.warning(log_messages)
            continue

        if job_state == JobState.JOB_ID_NOT_FOUND:
            end_time = time.time()
            time_elasped = int(end_time) - int(start_time)
            if time_elasped > time_out_for_failure:
                job_state = JobState.JOB_ID_NOT_FOUND
                job_message = (
                    f"Armada {airflow_task_name}:{job_id} could not find a job id and\n"
                    f"hit a timeout"
                )
                break

    return job_state, job_message


def annotate_job_request_items(
    context, job_request_items: List[submit_pb2.JobSubmitRequestItem]
) -> List[submit_pb2.JobSubmitRequestItem]:
    """
    Annotates the inbound job request items with Airflow context elements

    :param context: The airflow context.

    :param job_request_items: The job request items to be sent to armada

    :return: annotated job request items for armada
    """
    task_instance = context["ti"]
    task_id = task_instance.task_id
    run_id = context["run_id"]
    dag_id = context["dag"].dag_id

    for item in job_request_items:
        item.annotations[get_annotation_key_prefix() + "taskId"] = task_id
        item.annotations[get_annotation_key_prefix() + "taskRunId"] = run_id
        item.annotations[get_annotation_key_prefix() + "dagId"] = dag_id

    return job_request_items


ANNOTATION_KEY_PREFIX = "armadaproject.io/"


def get_annotation_key_prefix() -> str:
    """
    Provides the annotation key prefix,
    which can be specified in env var ANNOTATION_KEY_PREFIX.
    A default is provided if the env var is not defined

    :return: string annotation key prefix
    """
    env_var_name = "ANNOTATION_KEY_PREFIX"
    if env_var_name in os.environ:
        return f"{os.environ.get(env_var_name)}"
    else:
        return ANNOTATION_KEY_PREFIX


async def search_for_job_complete_async(
    armada_queue: str,
    job_set_id: str,
    airflow_task_name: str,
    job_id: str,
    job_service_client: JobServiceAsyncIOClient,
    time_out_for_failure: int = 7200,
) -> Tuple[JobState, str]:
    """

    Poll JobService cache asyncronously until you get a terminated event.

    A terminated event is SUCCEEDED, FAILED or CANCELLED

    :param armada_queue: The queue for armada
    :param job_set_id: Your job_set_id
    :param airflow_task_name: The name of your armada job
    :param job_id: The name of the job id that armada assigns to it
    :param job_service_client: A JobServiceClient that is used for polling.
                                It is optional only for testing
    :param time_out_for_failure: The amount of time a job
                                    can be in job_id_not_found
                                    before we decide it was a invalid job
    :return: A tuple of JobStateEnum, message
    """
    start_time = time.time()
    # Overwrite time_out_for_failure by environment variable for configuration
    armada_time_out_env = os.getenv("ARMADA_AIRFLOW_TIME_OUT_JOB_ID")
    if armada_time_out_env:
        time_out_for_failure = int(armada_time_out_env)
    while True:
        job_status_return = await job_service_client.get_job_status(
            queue=armada_queue,
            job_id=job_id,
            job_set_id=job_set_id,
        )

        job_state = job_state_from_pb(job_status_return.state)
        armada_logger.debug(f"Got job state '{job_state.name}' for job {job_id}")

        await asyncio.sleep(3)

        if job_state == JobState.SUCCEEDED:
            job_message = f"Armada {airflow_task_name}:{job_id} succeeded"
            break
        if job_state == JobState.FAILED:
            job_message = (
                f"Armada {airflow_task_name}:{job_id} failed\n"
                f"failed with reason {job_status_return.error}"
            )
            break
        if job_state == JobState.CANCELLED:
            job_message = f"Armada {airflow_task_name}:{job_id} cancelled"
            break
        if job_state == JobState.CONNECTION_ERR:
            log_messages = (
                f"Armada {airflow_task_name}:{job_id} connection error (will retry)"
                f"failed with reason {job_status_return.error}"
            )
            armada_logger.warning(log_messages)
            continue

        if job_state == JobState.JOB_ID_NOT_FOUND:
            end_time = time.time()
            time_elasped = int(end_time) - int(start_time)
            if time_elasped > time_out_for_failure:
                job_state = JobState.JOB_ID_NOT_FOUND
                job_message = (
                    f"Armada {airflow_task_name}:{job_id} could not find a job id and\n"
                    f"hit a timeout"
                )
                break

    return job_state, job_message
