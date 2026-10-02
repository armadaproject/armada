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


def hook_with_latest_run(run_id: str, cluster: str) -> tuple[ArmadaHook, MagicMock]:
    client = MagicMock()
    client.get_job_status.return_value.job_states = {"job-1": JobState.RUNNING.value}
    latest_run = MagicMock(run_id=run_id, cluster=cluster)
    job_details = MagicMock(latest_run_id=run_id, job_runs=[latest_run])
    client.get_job_details.return_value.job_details = {"job-1": job_details}
    return ArmadaHook(GrpcChannelArgs(target="api.armadaproject.io:443")), client


def running_context() -> RunningJobContext:
    return RunningJobContext(
        "queue",
        "job-1",
        "job-set",
        DateTime(2026, 1, 1),
        cluster="cluster-1",
        job_state=JobState.RUNNING.name,
        run_id="run-1",
        last_log_time=DateTime(2026, 1, 1, 12),
        pod_name="armada-run-1",
    )


def test_refresh_context_follows_a_new_run():
    hook, client = hook_with_latest_run("run-2", "cluster-2")

    with patch.object(ArmadaHook, "client", new=client):
        refreshed = hook.refresh_context(running_context(), "")

    assert refreshed.cluster == "cluster-2"
    assert refreshed.run_id == "run-2"
    assert refreshed.last_log_time is None
    assert refreshed.pod_name is None


def test_context_survives_the_xcom_round_trip():
    hook, client = hook_with_latest_run("run-1", "cluster-1")
    ti = MagicMock()

    hook.context_to_xcom(ti, running_context())
    pushed = ti.xcom_push.call_args.kwargs["value"]
    with (
        patch("armada.hooks.xcom_pull_for_ti", return_value=pushed),
        patch.object(ArmadaHook, "client", new=client),
    ):
        refreshed = hook.refresh_context(hook.context_from_xcom(ti), "")

    assert refreshed == running_context()
