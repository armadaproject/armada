from typing import Literal
from airflow.utils.context import Context

from armada_client.typings import JobState


def external_job_uri(context: Context) -> str:
    task_id = context["ti"].task_id
    map_index = context["ti"].map_index
    run_id = context["run_id"]
    dag_id = context["dag"].dag_id

    return f"airflow://{dag_id}/{task_id}/{run_id}/{map_index}"


def policy(policy_type: Literal["always", "never", "running_or_succeeded"]) -> callable:
    """
    Returns the corresponding re-attach policy function based on the policy type.

    :param policy_type: The type of policy ('always', 'never', 'running_or_succeeded').
    :type policy_type: Literal['always', 'never', 'running_or_succeeded']
    :return: A function that determines whether to re-attach to an existing job.
    :rtype: Callable[[JobState, str], bool]
    """
    policy_type = policy_type.lower()
    if policy_type == "always":
        return always_reattach
    elif policy_type == "never":
        return never_reattach
    elif policy_type == "running_or_succeeded":
        return running_or_succeeded_reattach
    else:
        raise ValueError(f"Unknown policy type: {policy_type}")


def never_reattach(state: JobState, termination_reason: str) -> bool:
    """
    Policy that never allows re-attaching a job.
    """
    return False


def always_reattach(state: JobState, termination_reason: str) -> bool:
    """
    Policy that always re-attaches to a job.
    """
    return True


def running_or_succeeded_reattach(state: JobState, termination_reason: str) -> bool:
    """
    Policy that allows re-attaching as long as it hasn't failed.
    """
    return state not in {JobState.FAILED, JobState.REJECTED}
