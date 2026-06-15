from unittest.mock import MagicMock, patch

import grpc
import pytest
from airflow.utils.state import TaskInstanceState
from armada._compat import deserialize, serialize
from armada.model import GrpcChannelArgs, RunningJobContext
from armada.triggers import ArmadaPollJobTrigger
from armada_client.typings import JobState
from pendulum import DateTime


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
async def test_cancels_running_job_when_task_is_cancelled():
    trigger = _make_trigger_with_task_state(TaskInstanceState.SUCCESS)
    job_ctx = RunningJobContext(
        "queue", "job-1", "job-set", DateTime.utcnow(), "cluster"
    )

    hook = MagicMock()
    hook.context_from_xcom.return_value = job_ctx

    with (
        patch.object(
            ArmadaPollJobTrigger,
            "_get_task_state",
            return_value=TaskInstanceState.SUCCESS,
        ),
        patch.object(
            ArmadaPollJobTrigger,
            "hook",
            new_callable=lambda: property(lambda self: hook),
        ),
    ):
        await trigger.on_kill()
        await trigger.cleanup()

    assert hook.cancel_job.call_count == 2
    hook.cancel_job.assert_called_with(job_ctx)


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
