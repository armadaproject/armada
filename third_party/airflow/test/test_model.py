import grpc
from airflow.serialization.serde import deserialize, serialize
from armada.model import GrpcChannelArgs, RunningJobContext
from armada_client.typings import JobState
from pendulum import DateTime


def test_roundtrip_running_job_context():
    context = RunningJobContext(
        "queue_123",
        "job_id_123",
        "job_set_id_123",
        "cluster-1.armada.localhost",
        DateTime.utcnow(),
        DateTime.utcnow().add(minutes=-2),
        JobState.RUNNING.name,
    )

    result = deserialize(serialize(context))
    assert context == result
    assert JobState.RUNNING == result.state


def test_roundtrip_grpc_channel_args():
    channel_args = GrpcChannelArgs(
        "armada-api.localhost",
        [("key-1", 10), ("key-2", "value-2")],
        grpc.Compression.NoCompression,
        None,
    )

    result = deserialize(serialize(channel_args))
    assert channel_args == result
