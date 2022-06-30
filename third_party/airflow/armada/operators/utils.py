from airflow.exceptions import AirflowFailException
from typing import Optional, Tuple

from armada.operators.jobservice import JobServiceClient
from armada.jobservice import jobservice_pb2


def airflow_error(job_state: str, name: str, job_id: str):
    """Throw an error on a terminal event if job errored out

    :param job_state: A string representation of state
    :param name: The name of your armada job
    :param job_id: The job id that armada assigns to it
    :return: No Return or an AirflowFailException.
    AirflowFailException tells Airflow Schedule to not reschedule the task
    """
    if job_state == "successful" or job_state == "running" or job_state == "queued":
        return
    if (
        job_state == "failed"
        or job_state == "cancelled"
        or job_state == "cancelling"
        or job_state == "terminated"
    ):
        raise AirflowFailException(f"The Armada job {name}:{job_id} {job_state}")


def default_job_status_callable(
    queue: str,
    job_set_id: str,
    job_id: str,
    job_service_client: Optional[JobServiceClient],
):
    return job_service_client.get_job_status(
        queue=queue, job_id=job_id, job_set_id=job_set_id
    )


def search_for_job_complete(
    queue: str,
    job_set_id: str,
    airflow_task_name: str,
    job_id: str,
    job_service_client: Optional[JobServiceClient] = None,
    job_status_callable=default_job_status_callable,
) -> Tuple[str, str]:
    """Poll JobService cache until you get a terminated event.
    A terminated event is SUCCEEDED, FAILED or CANCELLED

    :param job_set_id: Your job_set_id
    :param airflow_task_name: The name of your armada job
    :param job_id: The name of the job id that armada assigns to it
    :param job_service_client: A JobServiceClient that is used for polling.
                                It is optional only for testing
    :param job_status_callable: A callable object for test injection.
    :return: A tuple of state, message
    """

    while True:
        if job_service_client:
            job_status_return = job_status_callable(
                queue=queue,
                job_id=job_id,
                job_set_id=job_set_id,
                job_service_client=job_service_client,
            )
        else:
            job_status_return = job_status_callable(
                queue=queue, job_id=job_id, job_set_id=job_set_id
            )

        if job_status_return.state == jobservice_pb2.JobServiceResponse.SUCCEEDED:
            job_state = "succeeded"
            job_message = f"Armada {airflow_task_name}:{job_id} succeeded"
            break
        if job_status_return.state == jobservice_pb2.JobServiceResponse.FAILED:
            job_state = "failed"
            job_message = (
                f"Armada {airflow_task_name}:{job_id} failed\n"
                f"failed with reason {job_status_return.error}"
            )

            break
        if job_status_return.state == jobservice_pb2.JobServiceResponse.CANCELLED:
            job_state = "cancelled"
            job_message = f"Armada {airflow_task_name}:{job_id} cancelled"
            break

    return job_state, job_message
