import dataclasses
from datetime import timedelta
from typing import Optional, Callable
from unittest.mock import MagicMock, patch, ANY

import pytest
from airflow.exceptions import TaskDeferred
from armada.model import GrpcChannelArgs, RunningJobContext
from armada.operators.armada import ArmadaOperator
from armada.operators.errors import ArmadaOperatorJobFailedError
from armada.triggers import ArmadaPollJobTrigger
from armada_client.armada.submit_pb2 import JobSubmitRequestItem
from armada_client.typings import JobState
from pendulum import UTC, DateTime

DEFAULT_CURRENT_TIME = DateTime(2024, 8, 7, tzinfo=UTC)
DEFAULT_JOB_ID = "test_job"
DEFAULT_TASK_ID = "test_task_1"
DEFAULT_JOB_SET = "prefix-test_run_1"
DEFAULT_QUEUE = "test_queue_1"
DEFAULT_CLUSTER = "cluster-1"


def default_hook() -> MagicMock:
    mock = MagicMock()
    job_context = running_job_context()
    mock.submit_job.return_value = job_context
    mock.job_by_external_job_uri.return_value = None
    mock.job_termination_reason.return_value = "FAILED"
    mock.refresh_context.return_value = dataclasses.replace(
        job_context, job_state=JobState.SUCCEEDED.name, cluster=DEFAULT_CLUSTER
    )
    mock.cancel_job.return_value = dataclasses.replace(
        job_context, job_state=JobState.CANCELLED.name
    )
    mock.context_from_xcom.return_value = None

    return mock


@pytest.fixture(scope="function", autouse=True)
def mock_operator_dependencies():
    # We no-op time.sleep in tests.
    with (
        patch("time.sleep", return_value=None) as sleep,
        patch(
            "armada.log_manager.KubernetesPodLogManager.fetch_container_logs"
        ) as logs,
        patch(
            "armada.operators.armada.ArmadaOperator.hook", new_callable=default_hook
        ) as hook,
    ):
        yield sleep, logs, hook


@pytest.fixture
def context():
    mock_ti = MagicMock()
    mock_ti.task_id = DEFAULT_TASK_ID
    mock_ti.try_number = 1
    mock_ti.xcom_pull.return_value = None
    mock_ti.dag_id = "test_dag_1"
    mock_ti.run_id = "test_run_1"
    mock_ti.map_index = -1

    mock_dag = MagicMock()
    mock_dag.dag_id = "test_dag_1"

    context = {"ti": mock_ti, "run_id": "test_run_1", "dag": mock_dag}

    return context


def operator(
    job_request: JobSubmitRequestItem,
    deferrable: bool = False,
    job_acknowledgement_timeout_s: int = 30,
    container_logs: Optional[str] = None,
    reattach_policy: Optional[str] | Callable[[JobState, str], bool] = None,
) -> ArmadaOperator:
    operator = ArmadaOperator(
        armada_queue=DEFAULT_QUEUE,
        channel_args=GrpcChannelArgs(target="api.armadaproject.io:443"),
        container_logs=container_logs,
        deferrable=deferrable,
        job_acknowledgement_timeout=job_acknowledgement_timeout_s,
        job_request=job_request,
        job_set_prefix="prefix-",
        lookout_url_template="http://lookout.armadaproject.io/jobs?job_id=<job_id>",
        name="test",
        task_id=DEFAULT_TASK_ID,
        reattach_policy=reattach_policy,
    )

    return operator


def running_job_context(
    cluster: str = None,
    submit_time: DateTime = DateTime.now(),
    job_state: str = JobState.UNKNOWN.name,
) -> RunningJobContext:
    return RunningJobContext(
        DEFAULT_QUEUE,
        DEFAULT_JOB_ID,
        DEFAULT_JOB_SET,
        submit_time,
        cluster,
        job_state=job_state,
    )


@pytest.mark.parametrize(
    "job_states",
    [
        [JobState.RUNNING, JobState.SUCCEEDED],
        [
            JobState.QUEUED,
            JobState.LEASED,
            JobState.QUEUED,
            JobState.RUNNING,
            JobState.SUCCEEDED,
        ],
    ],
    ids=["success", "success - multiple events"],
)
def test_execute(job_states, context):
    op = operator(JobSubmitRequestItem())

    op.hook.refresh_context.side_effect = [
        running_job_context(cluster="cluster-1", job_state=s.name) for s in job_states
    ]

    op.execute(context)

    op.hook.submit_job.assert_called_once_with(
        DEFAULT_QUEUE, DEFAULT_JOB_SET, op.job_request
    )
    assert op.hook.refresh_context.call_count == len(job_states)

    # We're not polling for logs
    op.pod_manager.fetch_container_logs.assert_not_called()


@patch("pendulum.DateTime.utcnow", return_value=DEFAULT_CURRENT_TIME)
def test_execute_in_deferrable(_, context):
    op = operator(JobSubmitRequestItem(), deferrable=True)
    op.hook.refresh_context.side_effect = [
        running_job_context(cluster="cluster-1", job_state=s.name)
        for s in [JobState.QUEUED, JobState.QUEUED]
    ]

    with pytest.raises(TaskDeferred) as deferred:
        op.execute(context)

    op.hook.submit_job.assert_called_once_with(
        DEFAULT_QUEUE, DEFAULT_JOB_SET, op.job_request
    )
    assert deferred.value.timeout == op.execution_timeout
    # Trigger equality only checks moment, not context/channel_args
    assert deferred.value.trigger == ArmadaPollJobTrigger(
        moment=DEFAULT_CURRENT_TIME + timedelta(seconds=op.poll_interval),
    )
    assert deferred.value.method_name == "_trigger_reentry"


@pytest.mark.parametrize(
    "terminal_state",
    [JobState.FAILED, JobState.PREEMPTED, JobState.CANCELLED],
    ids=["failed", "preempted", "cancelled"],
)
def test_execute_fail(terminal_state, context):
    op = operator(JobSubmitRequestItem())

    op.hook.refresh_context.side_effect = [
        running_job_context(cluster="cluster-1", job_state=s.name)
        for s in [JobState.RUNNING, terminal_state]
    ]

    with pytest.raises(ArmadaOperatorJobFailedError) as exec_info:
        op.execute(context)

    # Error message contain terminal state and job id
    assert DEFAULT_JOB_ID in str(exec_info)
    assert terminal_state.name.capitalize() in str(exec_info)

    op.hook.submit_job.assert_called_once_with(
        DEFAULT_QUEUE, DEFAULT_JOB_SET, op.job_request
    )
    assert op.hook.refresh_context.call_count == 2

    # We're not polling for logs
    op.pod_manager.fetch_container_logs.assert_not_called()


@patch("armada.operators.armada.get_current_context")
def test_on_kill_terminates_running_job(mock_get_context):
    op = operator(JobSubmitRequestItem())
    job_context = running_job_context()

    # Mock get_current_context to return a context with ti
    mock_ti = MagicMock()
    mock_get_context.return_value = {"ti": mock_ti}

    # First call returns job_context, second call returns None (already cancelled)
    op.hook.context_from_xcom.side_effect = [job_context, None]

    op.on_kill()
    op.on_kill()

    # We ensure we only try to cancel job once (second call has no context).
    op.hook.cancel_job.assert_called_once_with(job_context)


def test_not_acknowledged_within_timeout_terminates_running_job(context):
    job_context = running_job_context()
    op = operator(JobSubmitRequestItem(), job_acknowledgement_timeout_s=-1)
    op.hook.refresh_context.return_value = job_context

    with pytest.raises(ArmadaOperatorJobFailedError) as exec_info:
        op.execute(context)

    # Error message contain terminal state and job id
    assert DEFAULT_JOB_ID in str(exec_info)
    assert JobState.CANCELLED.name.capitalize() in str(exec_info)

    # We also cancel already submitted job
    op.hook.cancel_job.assert_called_once_with(job_context)


def test_polls_for_logs(context):
    op = operator(
        JobSubmitRequestItem(namespace="namespace-1"), container_logs="alpine"
    )
    op.execute(context)

    # We polled logs as expected.
    op.pod_manager.fetch_container_logs.assert_called_once_with(
        k8s_context="cluster-1",
        namespace="namespace-1",
        pod="armada-test_job-0",
        container="alpine",
        since_time=None,
        link_extractor=ANY,
    )


def test_publishes_xcom_state(context):
    op = operator(JobSubmitRequestItem())
    op.execute(context)

    assert op.hook.context_to_xcom.call_count == 2


@pytest.mark.parametrize(
    "policy_return, should_reattach",
    [(True, True), (False, False)],
)
def test_reattaches_to_running_job(policy_return, should_reattach, context):
    # Simulate a retry
    context["ti"].try_number = 2
    context["ti"].max_tries = 5
    context["ti"].task.retries = 1

    op = operator(
        JobSubmitRequestItem(), reattach_policy=lambda state, reason: policy_return
    )

    expected_context = running_job_context(
        job_state=JobState.RUNNING.name, cluster=DEFAULT_CLUSTER
    )
    op.hook.job_by_external_job_uri.return_value = expected_context

    result = op.execute(context)

    if should_reattach:
        assert result["job_id"] == DEFAULT_JOB_ID
        assert result["job_state"] == JobState.SUCCEEDED.name
        op.hook.submit_job.assert_not_called()
    else:
        op.hook.submit_job.assert_called_once()


@pytest.mark.parametrize(
    "policy_return, should_reattach",
    [(True, True), (False, False)],
)
def test_reattaches_to_running_job_callable_policy(
    policy_return, should_reattach, context
):
    # Simulate a retry
    context["ti"].try_number = 2
    context["ti"].max_tries = 5
    context["ti"].task.retries = 1

    op = operator(
        JobSubmitRequestItem(), reattach_policy=lambda state, reason: policy_return
    )

    expected_context = running_job_context(
        job_state=JobState.RUNNING.name, cluster=DEFAULT_CLUSTER
    )
    op.hook.job_by_external_job_uri.return_value = expected_context

    result = op.execute(context)

    if should_reattach:
        assert result["job_id"] == DEFAULT_JOB_ID
        assert result["job_state"] == JobState.SUCCEEDED.name
        op.hook.submit_job.assert_not_called()
    else:
        op.hook.submit_job.assert_called_once()


@pytest.mark.skip("TODO")
def test_templates_job_request_item():
    pass
