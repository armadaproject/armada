from unittest.mock import Mock
import pytest

from armada_client.typings import JobState

from armada.policies.reattach import (
    policy,
    never_reattach,
    always_reattach,
    running_or_succeeded_reattach,
    external_job_uri,
)


def test_external_job_uri():
    mock_task_instance = Mock()
    mock_task_instance.task_id = "example_task"
    mock_task_instance.map_index = 42

    mock_dag = Mock()
    mock_dag.dag_id = "example_dag"

    mock_context = {"ti": mock_task_instance, "run_id": "test_run_123", "dag": mock_dag}

    expected_uri = "airflow://example_dag/example_task/test_run_123/42"

    assert external_job_uri(mock_context) == expected_uri


@pytest.mark.parametrize(
    "state",
    list(JobState),
)
def test_never_reattach(state):
    assert not never_reattach(state, termination_reason="any reason")


@pytest.mark.parametrize(
    "state",
    list(JobState),
)
def test_always_reattach(state):
    assert always_reattach(state, termination_reason="any reason")


@pytest.mark.parametrize(
    "state, expected",
    [
        (JobState.RUNNING, True),
        (JobState.SUCCEEDED, True),
        (JobState.CANCELLED, True),
        (JobState.FAILED, False),
        (JobState.REJECTED, False),
    ],
)
def test_running_or_succeeded_reattach(state, expected):
    assert (
        running_or_succeeded_reattach(state, termination_reason="any reason")
        == expected
    )


@pytest.mark.parametrize(
    "policy_type, expected_function",
    [
        ("always", always_reattach),
        ("never", never_reattach),
        ("running_or_succeeded", running_or_succeeded_reattach),
        ("ALWAYS", always_reattach),
    ],
)
def test_policy_selector(policy_type, expected_function):
    assert policy(policy_type) is expected_function


def test_policy_selector_invalid():
    with pytest.raises(ValueError, match="Unknown policy type: invalid"):
        policy("invalid")
