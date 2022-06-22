from airflow.exceptions import AirflowFailException
from typing import Tuple


def airflow_error(job_state: str, name: str, job_id: str):
    """Throw an error on a terminal event if job errored out

    :param job_state: A string representation of state
    :param name: The name of your armada job
    :param job_id: The job id that armada assigns to it
    :return: No Return or an AirflowFailException.  
    AirflowFailException tells Airflow Schedule to not reschedule the task

    if job_state == "successful" or job_state == "running" or job_state == "queued":
        return
    if (
        job_state == "failed"
        or job_state == "cancelled"
        or job_state == "cancelling"
        or job_state == "terminated"
    ):
        raise AirflowFailException(f"The Armada job {name}:{job_id} {job_state}")
    """
def search_for_job_complete(event, job_name: str, job_id: str) -> Tuple[str, str]:
    """Search the event stream to see if your job has finished running

    :param event: a gRPC event stream
    :param job_name: The name of your armada job
    :param job_id: The name of the job id that armada assigns to it
    :return: A tuple of state, message
    """

    job_state = "queued"
    job_message = f"Armada {job_name}:{job_id} is queued"
    for element in event:
        if element.message.succeeded.job_id == job_id:
            job_state = "successful"
            job_message = f"Armada {job_name}:{job_id} succeeded"
            break
        if element.message.failed.job_id == job_id:
            job_state = "failed"
            job_message = (
                f"Armada {job_name}:{job_id} failed\n"
                f"failed with reason {element.message.failed.reason}"
            )

            break
        if element.message.cancelling.job_id == job_id:
            job_state = "cancelling"
            job_message = f"Armada {job_name}:{job_id} cancelling"
            break
        if element.message.cancelled.job_id == job_id:
            job_state = "cancelled"
            job_message = f"Armada {job_name}:{job_id} cancelled"
            break
        if element.message.terminated.job_id == job_id:
            job_state = "terminated"
            job_message = f"Armada {job_name}:{job_id} terminated"
            break

    return job_state, job_message
