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
