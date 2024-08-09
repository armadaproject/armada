import unittest
from datetime import timedelta
from math import ceil
from unittest.mock import MagicMock, PropertyMock, patch

from airflow.exceptions import AirflowException
from armada.model import GrpcChannelArgs
from armada.operators.armada import (
    ArmadaOperator,
    _ArmadaPollJobTrigger,
    _RunningJobContext,
)
from armada_client.armada import job_pb2, submit_pb2
from armada_client.armada.submit_pb2 import JobSubmitRequestItem
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)
from armada_client.typings import JobState
from pendulum import UTC, DateTime

DEFAULT_CURRENT_TIME = DateTime(2024, 8, 7, tzinfo=UTC)
DEFAULT_JOB_ID = "test_job"
DEFAULT_TASK_ID = "test_task_1"
DEFAULT_DAG_ID = "test_dag_1"
DEFAULT_RUN_ID = "test_run_1"
DEFAULT_QUEUE = "test_queue_1"
DEFAULT_POLLING_INTERVAL = 30
DEFAULT_JOB_ACKNOWLEDGEMENT_TIMEOUT = 5 * 60


class TestArmadaOperator(unittest.TestCase):
    def setUp(self):
        # Set up a mock context
        mock_ti = MagicMock()
        mock_ti.task_id = DEFAULT_TASK_ID
        mock_dag = MagicMock()
        mock_dag.dag_id = DEFAULT_DAG_ID
        self.context = {
            "ti": mock_ti,
            "run_id": DEFAULT_RUN_ID,
            "dag": mock_dag,
        }

    @patch("time.sleep", return_value=None)
    @patch("armada.operators.armada.ArmadaOperator.client", new_callable=PropertyMock)
    def test_execute(self, mock_client_fn, _):
        test_cases = [
            {
                "name": "Job Succeeds",
                "statuses": [submit_pb2.RUNNING, submit_pb2.SUCCEEDED],
                "success": True,
            },
            {
                "name": "Job Failed",
                "statuses": [submit_pb2.RUNNING, submit_pb2.FAILED],
                "success": False,
            },
            {
                "name": "Job cancelled",
                "statuses": [submit_pb2.RUNNING, submit_pb2.CANCELLED],
                "success": False,
            },
            {
                "name": "Job preempted",
                "statuses": [submit_pb2.RUNNING, submit_pb2.PREEMPTED],
                "success": False,
            },
            {
                "name": "Job Succeeds but takes a lot of transitions",
                "statuses": [
                    submit_pb2.SUBMITTED,
                    submit_pb2.RUNNING,
                    submit_pb2.RUNNING,
                    submit_pb2.RUNNING,
                    submit_pb2.RUNNING,
                    submit_pb2.RUNNING,
                    submit_pb2.SUCCEEDED,
                ],
                "success": True,
            },
        ]

        for test_case in test_cases:
            with self.subTest(test_case=test_case["name"]):
                operator = ArmadaOperator(
                    name="test",
                    channel_args=GrpcChannelArgs(target="api.armadaproject.io:443"),
                    armada_queue=DEFAULT_QUEUE,
                    job_request=JobSubmitRequestItem(),
                    task_id=DEFAULT_TASK_ID,
                )

                #  Set up Mock Armada
                mock_client = MagicMock()
                mock_client.submit_jobs.return_value = submit_pb2.JobSubmitResponse(
                    job_response_items=[
                        submit_pb2.JobSubmitResponseItem(job_id=DEFAULT_JOB_ID)
                    ]
                )

                mock_client.get_job_status.side_effect = [
                    job_pb2.JobStatusResponse(job_states={DEFAULT_JOB_ID: x})
                    for x in test_case["statuses"]
                ]

                mock_client_fn.return_value = mock_client
                self.context["ti"].xcom_pull.return_value = None

                try:
                    operator.execute(self.context)
                    self.assertTrue(test_case["success"])
                except AirflowException:
                    self.assertFalse(test_case["success"])
                    return

                self.assertEqual(mock_client.submit_jobs.call_count, 1)
                self.assertEqual(
                    mock_client.get_job_status.call_count, len(test_case["statuses"])
                )

    @patch("time.sleep", return_value=None)
    @patch(
        "armada.operators.armada.ArmadaOperator._cancel_job", new_callable=PropertyMock
    )
    @patch("armada.operators.armada.ArmadaOperator.client", new_callable=PropertyMock)
    def test_unacknowledged_results_in_on_kill(self, mock_client_fn, mock_on_kill, _):
        operator = ArmadaOperator(
            name="test",
            channel_args=GrpcChannelArgs(target="api.armadaproject.io:443"),
            armada_queue=DEFAULT_QUEUE,
            job_request=JobSubmitRequestItem(),
            task_id=DEFAULT_TASK_ID,
            deferrable=False,
            job_acknowledgement_timeout=-1,
        )

        #  Set up Mock Armada
        mock_client = MagicMock()
        mock_client.submit_jobs.return_value = submit_pb2.JobSubmitResponse(
            job_response_items=[submit_pb2.JobSubmitResponseItem(job_id=DEFAULT_JOB_ID)]
        )
        mock_client_fn.return_value = mock_client
        mock_client.get_job_status.side_effect = [
            job_pb2.JobStatusResponse(job_states={DEFAULT_JOB_ID: x})
            for x in [submit_pb2.UNKNOWN, submit_pb2.UNKNOWN]
        ]

        self.context["ti"].xcom_pull.return_value = None
        with self.assertRaises(AirflowException):
            operator.execute(self.context)
        self.assertEqual(mock_on_kill.call_count, 1)

    """We call on_kill by triggering the job unacknowledged timeout"""

    @patch("time.sleep", return_value=None)
    @patch("armada.operators.armada.ArmadaOperator.client", new_callable=PropertyMock)
    def test_on_kill_cancels_job(self, mock_client_fn, _):
        operator = ArmadaOperator(
            name="test",
            channel_args=GrpcChannelArgs(target="api.armadaproject.io:443"),
            armada_queue=DEFAULT_QUEUE,
            job_request=JobSubmitRequestItem(),
            task_id=DEFAULT_TASK_ID,
            deferrable=False,
            job_acknowledgement_timeout=-1,
        )

        #  Set up Mock Armada
        mock_client = MagicMock()
        mock_client.submit_jobs.return_value = submit_pb2.JobSubmitResponse(
            job_response_items=[submit_pb2.JobSubmitResponseItem(job_id=DEFAULT_JOB_ID)]
        )
        mock_client_fn.return_value = mock_client
        mock_client.get_job_status.side_effect = [
            job_pb2.JobStatusResponse(job_states={DEFAULT_JOB_ID: x})
            for x in [
                submit_pb2.UNKNOWN
                for _ in range(
                    1
                    + ceil(
                        DEFAULT_JOB_ACKNOWLEDGEMENT_TIMEOUT / DEFAULT_POLLING_INTERVAL
                    )
                )
            ]
        ]

        self.context["ti"].xcom_pull.return_value = None
        with self.assertRaises(AirflowException):
            operator.execute(self.context)
        self.assertEqual(mock_client.cancel_jobs.call_count, 1)

    @patch("time.sleep", return_value=None)
    @patch("armada.operators.armada.ArmadaOperator.client", new_callable=PropertyMock)
    def test_job_reattaches(self, mock_client_fn, _):
        operator = ArmadaOperator(
            name="test",
            channel_args=GrpcChannelArgs(target="api.armadaproject.io:443"),
            armada_queue=DEFAULT_QUEUE,
            job_request=JobSubmitRequestItem(),
            task_id=DEFAULT_TASK_ID,
            deferrable=False,
            job_acknowledgement_timeout=10,
        )

        #  Set up Mock Armada
        mock_client = MagicMock()
        mock_client.get_job_status.side_effect = [
            job_pb2.JobStatusResponse(job_states={DEFAULT_JOB_ID: x})
            for x in [
                submit_pb2.SUCCEEDED
                for _ in range(
                    1
                    + ceil(
                        DEFAULT_JOB_ACKNOWLEDGEMENT_TIMEOUT / DEFAULT_POLLING_INTERVAL
                    )
                )
            ]
        ]
        mock_client_fn.return_value = mock_client
        self.context["ti"].xcom_pull.return_value = {"armada_job_id": DEFAULT_JOB_ID}

        operator.execute(self.context)
        self.assertEqual(mock_client.submit_jobs.call_count, 0)


class TestArmadaOperatorDeferrable(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Set up a mock context
        mock_ti = MagicMock()
        mock_ti.task_id = DEFAULT_TASK_ID
        mock_dag = MagicMock()
        mock_dag.dag_id = DEFAULT_DAG_ID
        self.context = {
            "ti": mock_ti,
            "run_id": DEFAULT_RUN_ID,
            "dag": mock_dag,
        }

    @patch("pendulum.DateTime.utcnow")
    @patch("armada.operators.armada.ArmadaOperator.defer")
    @patch("armada.operators.armada.ArmadaOperator.client", new_callable=PropertyMock)
    def test_execute_deferred(self, mock_client_fn, mock_defer_fn, mock_datetime_now):
        operator = ArmadaOperator(
            name="test",
            channel_args=GrpcChannelArgs(target="api.armadaproject.io:443"),
            armada_queue=DEFAULT_QUEUE,
            job_request=JobSubmitRequestItem(),
            task_id=DEFAULT_TASK_ID,
            deferrable=True,
        )

        mock_datetime_now.return_value = DEFAULT_CURRENT_TIME

        #  Set up Mock Armada
        mock_client = MagicMock()
        mock_client.submit_jobs.return_value = submit_pb2.JobSubmitResponse(
            job_response_items=[submit_pb2.JobSubmitResponseItem(job_id=DEFAULT_JOB_ID)]
        )
        mock_client_fn.return_value = mock_client
        self.context["ti"].xcom_pull.return_value = None

        operator.execute(self.context)
        self.assertEqual(mock_client.submit_jobs.call_count, 1)
        mock_defer_fn.assert_called_with(
            timeout=operator.execution_timeout,
            trigger=_ArmadaPollJobTrigger(
                moment=DEFAULT_CURRENT_TIME + timedelta(seconds=operator.poll_interval),
                context=_RunningJobContext(
                    armada_queue=DEFAULT_QUEUE,
                    job_set_id=operator.job_set_id,
                    job_id=DEFAULT_JOB_ID,
                    state=JobState.UNKNOWN,
                    start_time=DEFAULT_CURRENT_TIME,
                    cluster=None,
                    last_log_time=None,
                ),
            ),
            method_name="_deffered_poll_for_termination",
        )

    def test_templating(self):
        """Tests templating for both the job_prefix and the pod spec"""
        prefix = "{{ run_id  }}"
        pod_arg = "{{ run_id }}"

        pod = core_v1.PodSpec(
            containers=[
                core_v1.Container(
                    name="sleep",
                    image="alpine:3.20.2",
                    args=[pod_arg],
                    securityContext=core_v1.SecurityContext(runAsUser=1000),
                    resources=core_v1.ResourceRequirements(
                        requests={
                            "cpu": api_resource.Quantity(string="120m"),
                            "memory": api_resource.Quantity(string="510Mi"),
                        },
                        limits={
                            "cpu": api_resource.Quantity(string="120m"),
                            "memory": api_resource.Quantity(string="510Mi"),
                        },
                    ),
                )
            ],
        )
        job = JobSubmitRequestItem(priority=1, pod_spec=pod, namespace="armada")

        operator = ArmadaOperator(
            name="test",
            channel_args=GrpcChannelArgs(target="api.armadaproject.io:443"),
            armada_queue=DEFAULT_QUEUE,
            job_request=job,
            job_set_prefix=prefix,
            task_id=DEFAULT_TASK_ID,
            deferrable=True,
        )

        operator.render_template_fields(self.context)

        self.assertEqual(operator.job_set_prefix, "test_run_1")
        self.assertEqual(
            operator.job_request.pod_spec.containers[0].args[0], "test_run_1"
        )
