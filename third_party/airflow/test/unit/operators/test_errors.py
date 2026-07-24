import pytest
from armada_client.typings import JobState
from armada._compat import AirflowFailException
from armada.operators.errors import (
    ArmadaOperatorJobFailedError,
    ArmadaOperatorJobFailedFatalError,
)


def test_constructor():
    job_id = "test-job"
    queue = "default-queue"
    state = JobState.FAILED
    reason = "Out of memory"

    error = ArmadaOperatorJobFailedError(queue, job_id, state, reason)

    assert error.job_id == job_id
    assert error.queue == queue
    assert error.state == state
    assert error.reason == reason


@pytest.mark.parametrize(
    "reason,expected_message",
    [
        (
            "",
            "ArmadaOperator job 'test-job' in queue 'default-queue' terminated "
            "with state 'Failed'.",
        ),
        (
            "Out of memory",
            "ArmadaOperator job 'test-job' in queue 'default-queue' terminated "
            "with state 'Failed'. Termination reason: Out of memory",
        ),
    ],
)
def test_message(reason: str, expected_message: str):
    error = ArmadaOperatorJobFailedError(
        "default-queue", "test-job", JobState.FAILED, reason
    )
    assert str(error) == expected_message


def test_fatal_error_fails_airflow_task_without_retry():
    error = ArmadaOperatorJobFailedFatalError(
        "default-queue", "test-job", JobState.REJECTED, "Invalid pod spec"
    )

    assert isinstance(error, AirflowFailException)
    assert isinstance(error, ArmadaOperatorJobFailedError)
    assert error.queue == "default-queue"
    assert error.job_id == "test-job"
    assert error.state == JobState.REJECTED
    assert error.reason == "Invalid pod spec"
    assert str(error) == (
        "ArmadaOperator job 'test-job' in queue 'default-queue' terminated "
        "with state 'Rejected'. Termination reason: Invalid pod spec"
    )
