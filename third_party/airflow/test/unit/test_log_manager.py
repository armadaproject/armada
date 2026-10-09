from unittest.mock import MagicMock, patch

import pytest
import tenacity
from armada.log_manager import KubernetesPodLogManager
from kubernetes.client.exceptions import ApiException


def pods(*names):
    items = []
    for name in names:
        pod = MagicMock()
        pod.metadata.name = name
        items.append(pod)
    return MagicMock(items=items)


@pytest.mark.parametrize(
    "responses, expected",
    [
        pytest.param([pods("armada-run-1")], "armada-run-1", id="one pod of the run"),
        pytest.param([pods()], None, id="no pod of the run yet"),
        pytest.param([pods("armada-run-1", "copy")], None, id="two pods of the run"),
        pytest.param([ApiException(status=403)], None, id="no permission to list"),
        pytest.param(
            [ApiException(status=503), pods("armada-run-1")],
            "armada-run-1",
            id="a temporary API error is retried",
        ),
    ],
)
def test_pod_name_for_run(responses, expected):
    client = MagicMock()
    client.list_namespaced_pod.side_effect = responses

    with (
        patch.object(KubernetesPodLogManager, "_k8s_client", return_value=client),
        patch.object(
            KubernetesPodLogManager.pod_name_for_run.retry, "wait", tenacity.wait_none()
        ),
    ):
        name = KubernetesPodLogManager().pod_name_for_run(
            k8s_context="cluster-1", namespace="ns", job_id="job-1", run_id="run-1"
        )

    assert name == expected
    assert client.list_namespaced_pod.call_count == len(responses)
    client.list_namespaced_pod.assert_called_with(
        namespace="ns", label_selector="armada_job_id=job-1,armada_job_run_id=run-1"
    )
