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


JOB_ID = "01hcq4x7d2k9m3n5p8r0s1t2v4"
DEFAULT_POD_NAME = f"armada-{JOB_ID}-0"
LAST_LOG_TIME = DateTime(2026, 1, 1, 1)


NO_FIELD = object()


def run_details(run_id, cluster, pod_name=NO_FIELD, started=False):
    """Return run details. Without pod_name, they come from an armada-client
    that has no pod_name field."""
    fields = ["run_id", "cluster", "HasField"]
    if pod_name is not NO_FIELD:
        fields.append("pod_name")
    run = MagicMock(spec=fields, run_id=run_id, cluster=cluster)
    if pod_name is not NO_FIELD:
        run.pod_name = pod_name
    run.HasField.side_effect = lambda field: started and field == "started_ts"
    return run


@pytest.mark.parametrize(
    "latest_run, context_pod_name, want_pod_name, want_last_log_time",
    [
        pytest.param(
            run_details("run-2", "c-2", pod_name="armada-run-2"),
            "armada-run-1",
            "armada-run-2",
            None,
            id="a new pod clears the log cursor",
        ),
        pytest.param(
            run_details("run-1", "c-1", pod_name="armada-run-1"),
            "armada-run-1",
            "armada-run-1",
            LAST_LOG_TIME,
            id="the same pod keeps the log cursor",
        ),
        pytest.param(
            run_details("run-1", "c-1", pod_name="armada-run-1"),
            None,
            "armada-run-1",
            LAST_LOG_TIME,
            id="a context from an older operator keeps its log cursor",
        ),
        pytest.param(
            run_details("run-1", "c-1", pod_name="", started=True),
            DEFAULT_POD_NAME,
            DEFAULT_POD_NAME,
            LAST_LOG_TIME,
            id="a server without pod names gives the default name to a run with a pod",
        ),
        pytest.param(
            run_details("run-1", "c-1", started=True),
            DEFAULT_POD_NAME,
            DEFAULT_POD_NAME,
            LAST_LOG_TIME,
            id="a client without pod names gives the default name to a run with a pod",
        ),
        pytest.param(
            run_details("run-1", "c-1", pod_name=""),
            None,
            "",
            LAST_LOG_TIME,
            id="a run without a pod has no pod name",
        ),
    ],
)
def test_refresh_context(
    latest_run, context_pod_name, want_pod_name, want_last_log_time
):
    client = MagicMock()
    client.get_job_status.return_value.job_states = {JOB_ID: JobState.RUNNING.value}
    client.get_job_details.return_value.job_details = {
        JOB_ID: MagicMock(latest_run_id=latest_run.run_id, job_runs=[latest_run])
    }
    job_context = RunningJobContext(
        "queue",
        JOB_ID,
        "job-set",
        DateTime(2026, 1, 1),
        job_state=JobState.RUNNING.name,
        pod_name=context_pod_name,
        last_log_time=LAST_LOG_TIME,
    )

    with patch.object(ArmadaHook, "client", new=client):
        refreshed = ArmadaHook(
            GrpcChannelArgs(target="localhost:50051")
        ).refresh_context(job_context, "")

    assert refreshed.cluster == latest_run.cluster
    assert refreshed.pod_name == want_pod_name
    assert refreshed.last_log_time == want_last_log_time


def test_context_keeps_the_pod_name_through_xcom():
    ti = MagicMock()
    hook = ArmadaHook(GrpcChannelArgs(target="localhost:50051"))
    job_context = RunningJobContext(
        "queue", JOB_ID, "job-set", DateTime(2026, 1, 1), pod_name=DEFAULT_POD_NAME
    )

    hook.context_to_xcom(ti, job_context)
    pushed = ti.xcom_push.call_args.kwargs["value"]
    with patch("armada.hooks.xcom_pull_for_ti", return_value=pushed):
        restored = hook.context_from_xcom(ti)

    assert restored.pod_name == DEFAULT_POD_NAME
