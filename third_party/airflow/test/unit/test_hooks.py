from unittest.mock import MagicMock, patch

import pytest
from armada.hooks import ArmadaHook
from armada.model import GrpcChannelArgs, RunningJobContext
from armada_client.typings import JobState
from pendulum import DateTime


@pytest.mark.skip("TODO")
def test_submits_job_using_armada_client():
    pass


@pytest.mark.skip("TODO")
def test_cancels_job_using_armada_client():
    pass


@pytest.mark.skip("TODO")
def test_updates_job_context():
    pass


@pytest.mark.parametrize(
    "latest_pod_name, expected_pod_name",
    [
        pytest.param("armada-job-1-run-2", "armada-job-1-run-2", id="pod of the run"),
        pytest.param("", None, id="run without a pod yet"),
    ],
)
def test_refresh_context_reads_the_latest_run(latest_pod_name, expected_pod_name):
    client = MagicMock()
    client.get_job_status.return_value.job_states = {"job-1": JobState.RUNNING.value}
    latest_run = MagicMock(
        run_id="run-2", cluster="cluster-2", pod_name=latest_pod_name
    )
    client.get_job_details.return_value.job_details = {
        "job-1": MagicMock(latest_run_id="run-2", job_runs=[latest_run])
    }
    job_context = RunningJobContext(
        "queue",
        "job-1",
        "job-set",
        DateTime(2026, 1, 1),
        cluster="cluster-1",
        job_state=JobState.RUNNING.name,
        pod_name="armada-job-1-run-1",
    )

    with patch.object(ArmadaHook, "client", new=client):
        refreshed = ArmadaHook(
            GrpcChannelArgs(target="localhost:50051")
        ).refresh_context(job_context, "")

    assert refreshed.cluster == "cluster-2"
    assert refreshed.pod_name == expected_pod_name
