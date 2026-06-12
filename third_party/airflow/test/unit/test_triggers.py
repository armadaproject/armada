import grpc
import pytest
from armada.model import GrpcChannelArgs, RunningJobContext
from armada.triggers import ArmadaPollJobTrigger
from armada_client.typings import JobState
from pendulum import DateTime


def test_serialize_roundtrip_preserves_context_and_channel_args():
    # Exercises the deferred-trigger serde path end to end: serialize() encodes the
    # context/channel_args via serde, and reconstructing the trigger from the
    # serialized kwargs deserializes them back. This only works if Armada's context
    # types are registered in serde's allow-list (see armada.model), so it guards
    # against that registration regressing - including the Airflow 3.2 serde move.
    moment = DateTime.utcnow()
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

    classpath, kwargs = ArmadaPollJobTrigger(moment, context, channel_args).serialize()

    assert classpath == "armada.triggers.ArmadaPollJobTrigger"
    restored = ArmadaPollJobTrigger(**kwargs)
    assert restored.moment == moment
    assert restored.context == context
    assert restored.channel_args == channel_args


@pytest.mark.skip("TODO")
def test_yields_with_context():
    pass


@pytest.mark.skip("TODO")
def test_cancels_running_job_when_task_is_cancelled():
    pass


@pytest.mark.skip("TODO")
def test_do_not_cancels_running_job_when_trigger_is_suspended():
    pass
