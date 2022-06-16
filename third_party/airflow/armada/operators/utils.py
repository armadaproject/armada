from airflow.exceptions import AirflowException


def airflow_error(job_state: str, name: str, job_id: str):
    if job_state == "successful" or job_state == "running" or job_state == "queued":
        return
    if (
        job_state == "failed"
        or job_state == "cancelled"
        or job_state == "cancelling"
        or job_state == "terminated"
    ):
        raise AirflowException(f"The Armada job {name}:{job_id} {job_state}")


def search_for_job_complete(event, job_name: str, job_id: str, armada_logger) -> str:
    job_state = "queued"
    for element in event:
        if element.message.succeeded.job_id == job_id:
            armada_logger.info(
                f"Armada job {job_name} with id {job_id} completed without error"
            )
            job_state = "successful"
            break
        if element.message.running.job_id == job_id:
            armada_logger.info(f"Armada job {job_name} with id {job_id} running")
            job_state = "running"

        if element.message.failed.job_id == job_id:
            armada_logger.error
            (
                f"Armada job {job_name} with id {job_id}\n"
                f"failed with reason {element.message.failed.reason}"
            )
            job_state = "failed"
            break
        if element.message.cancelling.job_id == job_id:
            armada_logger.error(f"Armada job {job_name} with id {job_id} cancelling")
            job_state = "cancelling"
            break
        if element.message.cancelled.job_id == job_id:
            armada_logger.error(f"Armada job {job_name} with id {job_id} cancelled")
            job_state = "cancelled"
            break
        if element.message.terminated.job_id == job_id:
            armada_logger.error(f"Armada job {job_name} with id {job_id} terminated")
            job_state = "terminated"
            break

    return job_state
