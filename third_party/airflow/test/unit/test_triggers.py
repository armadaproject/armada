from unittest.mock import MagicMock, patch

import grpc
import pytest
from airflow.utils.state import TaskInstanceState
from armada._compat import AIRFLOW_V_3_0_PLUS, deserialize, serialize
from armada.model import GrpcChannelArgs, RunningJobContext
from armada.triggers import CANCEL_JOBS_WHEN_TASK_IN_STATE, ArmadaPollJobTrigger
from armada_client.typings import JobState
from pendulum import DateTime

GET_TASK_STATES = (
    "airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_task_states"
)

airflow_3_only = pytest.mark.skipif(
    not AIRFLOW_V_3_0_PLUS,
    reason="The Execution API state lookup only exists on Airflow 3",
)


def _api_server_error(status_code: int) -> Exception:
    """Build the error the Execution API raises when a call fails."""
    from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
    from airflow.sdk.execution_time.comms import ErrorResponse

    return AirflowRuntimeError(
        ErrorResponse(
            error=ErrorType.API_SERVER_ERROR,
            detail={"status_code": status_code, "message": "Not Found"},
        )
    )


def test_trigger_serialize_roundtrip_preserves_moment():
    moment = DateTime.utcnow()

    classpath, kwargs = ArmadaPollJobTrigger(moment).serialize()

    assert classpath == "armada.triggers.ArmadaPollJobTrigger"
    restored = ArmadaPollJobTrigger(**kwargs)
    assert restored.moment == moment


def test_serde_roundtrip_for_context_and_channel_args():
    # Guards the serde allow-list registration in armada.model: serde refuses to
    # deserialize unknown classes, so RunningJobContext and GrpcChannelArgs must be
    # in _extra_allowed. This exercises the same path used when these types cross
    # the xcom boundary, and protects against the registration regressing -
    # including the Airflow 3.2 serde module move.
    context = RunningJobContext(
        "queue_123",
        "job_id_123",
        "job_set_id_123",
        DateTime.utcnow(),
        "cluster-1.armada.localhost",
        job_state=JobState.RUNNING.name,
    )
    channel_args = GrpcChannelArgs(
        "armada-api.localhost",
        [("key-1", 10)],
        grpc.Compression.NoCompression,
        None,
    )

    assert deserialize(serialize(context)) == context
    assert deserialize(serialize(channel_args)) == channel_args


def _make_trigger_with_task_state(state: TaskInstanceState) -> ArmadaPollJobTrigger:
    trigger = ArmadaPollJobTrigger(DateTime.utcnow())
    ti = MagicMock()
    ti.dag_id = "dag"
    ti.task_id = "task"
    ti.run_id = "run"
    ti.map_index = -1
    ti.state = state
    trigger.task_instance = ti
    return trigger


@pytest.mark.asyncio
@pytest.mark.parametrize("state", sorted(CANCEL_JOBS_WHEN_TASK_IN_STATE))
async def test_cancels_running_job_when_task_in_terminal_state(state):
    trigger = _make_trigger_with_task_state(state)
    job_ctx = RunningJobContext(
        "queue", "job-1", "job-set", DateTime.utcnow(), "cluster"
    )

    hook = MagicMock()
    hook.context_from_xcom.return_value = job_ctx

    with (
        patch.object(ArmadaPollJobTrigger, "_get_task_state", return_value=state),
        patch.object(
            ArmadaPollJobTrigger,
            "hook",
            new_callable=lambda: property(lambda self: hook),
        ),
    ):
        await trigger.cleanup()

    hook.cancel_job.assert_called_once_with(job_ctx)


@pytest.mark.asyncio
async def test_do_not_cancels_running_job_when_trigger_is_suspended():
    trigger = _make_trigger_with_task_state(TaskInstanceState.DEFERRED)
    hook = MagicMock()

    with (
        patch.object(
            ArmadaPollJobTrigger,
            "_get_task_state",
            return_value=TaskInstanceState.DEFERRED,
        ),
        patch.object(
            ArmadaPollJobTrigger,
            "hook",
            new_callable=lambda: property(lambda self: hook),
        ),
    ):
        await trigger.cleanup()

    hook.cancel_job.assert_not_called()


@pytest.mark.asyncio
async def test_do_not_cancels_running_job_when_task_instance_was_removed():
    # Clearing a task supersedes the attempt this trigger belongs to. REMOVED says
    # nothing about the job having finished, and the replacement attempt owns the
    # xcom job context by then, so cleanup must leave the job running.
    trigger = _make_trigger_with_task_state(TaskInstanceState.REMOVED)
    hook = MagicMock()

    with (
        patch.object(
            ArmadaPollJobTrigger,
            "_get_task_state",
            return_value=TaskInstanceState.REMOVED,
        ),
        patch.object(
            ArmadaPollJobTrigger,
            "hook",
            new_callable=lambda: property(lambda self: hook),
        ),
    ):
        await trigger.cleanup()

    hook.cancel_job.assert_not_called()


@pytest.mark.asyncio
@airflow_3_only
async def test_task_state_lookup_reports_removed_when_lookup_returns_404():
    trigger = _make_trigger_with_task_state(TaskInstanceState.DEFERRED)

    with patch(GET_TASK_STATES, side_effect=_api_server_error(404)):
        assert await trigger._get_task_state() == TaskInstanceState.REMOVED


@pytest.mark.asyncio
@airflow_3_only
@pytest.mark.parametrize(
    "response",
    [
        pytest.param({"run": {}}, id="task_instance_absent_from_response"),
        pytest.param({}, id="dag_run_absent_from_response"),
        # Clearing a deferred task resets its state to NULL, which the Execution
        # API reports as a null state for the task instance.
        pytest.param({"run": {"task": None}}, id="state_reset_to_null_by_clear"),
    ],
)
async def test_task_state_lookup_reports_removed_when_state_is_unresolvable(response):
    trigger = _make_trigger_with_task_state(TaskInstanceState.DEFERRED)

    with patch(GET_TASK_STATES, return_value=response):
        assert await trigger._get_task_state() == TaskInstanceState.REMOVED


@pytest.mark.asyncio
@airflow_3_only
async def test_task_state_lookup_re_raises_non_404_api_errors():
    trigger = _make_trigger_with_task_state(TaskInstanceState.DEFERRED)
    api_error = _api_server_error(500)

    with patch(GET_TASK_STATES, side_effect=api_error):
        with pytest.raises(type(api_error)):
            await trigger._get_task_state()


@pytest.mark.asyncio
@airflow_3_only
@pytest.mark.parametrize("map_index, ti_key", [(-1, "task"), (3, "task_3")])
async def test_task_state_lookup_reads_state_from_response(map_index, ti_key):
    trigger = _make_trigger_with_task_state(TaskInstanceState.RUNNING)
    trigger.task_instance.map_index = map_index

    with patch(
        GET_TASK_STATES,
        return_value={"run": {ti_key: TaskInstanceState.RUNNING}},
    ):
        assert await trigger._get_task_state() == TaskInstanceState.RUNNING


@pytest.mark.asyncio
async def test_cleanup_re_raises_when_task_state_lookup_fails():
    trigger = _make_trigger_with_task_state(TaskInstanceState.SUCCESS)
    hook = MagicMock()

    with (
        patch.object(
            ArmadaPollJobTrigger,
            "_get_task_state",
            side_effect=RuntimeError("api down"),
        ),
        patch.object(
            ArmadaPollJobTrigger,
            "hook",
            new_callable=lambda: property(lambda self: hook),
        ),
    ):
        with pytest.raises(RuntimeError, match="api down"):
            await trigger.cleanup()

    hook.cancel_job.assert_not_called()
