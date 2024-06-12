import unittest
from unittest.mock import AsyncMock, patch, PropertyMock

from airflow.triggers.base import TriggerEvent
from armada_client.armada.submit_pb2 import JobState
from armada_client.armada import submit_pb2, job_pb2

from armada.model import GrpcChannelArgs
from armada.triggers.armada import ArmadaTrigger

DEFAULT_JOB_ID = "test_job"
DEFAULT_POLLING_INTERVAL = 30
DEFAULT_JOB_ACKNOWLEDGEMENT_TIMEOUT = 5 * 60


class AsyncMock(unittest.mock.MagicMock):  # noqa: F811
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


class TestArmadaTrigger(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.time = 0

    def test_serialization(self):
        trigger = ArmadaTrigger(
            job_id=DEFAULT_JOB_ID,
            channel_args=GrpcChannelArgs(target="api.armadaproject.io:443"),
            poll_interval=30,
            tracking_message="test tracking message",
            job_acknowledgement_timeout=DEFAULT_JOB_ACKNOWLEDGEMENT_TIMEOUT,
            job_request_namespace="default",
        )
        classpath, kwargs = trigger.serialize()
        self.assertEqual("armada.triggers.armada.ArmadaTrigger", classpath)

        rehydrated = ArmadaTrigger(**kwargs)
        self.assertEqual(trigger, rehydrated)

    def _time_side_effect(self):
        self.time += DEFAULT_POLLING_INTERVAL
        return self.time

    @patch("time.time")
    @patch("asyncio.sleep", new_callable=AsyncMock)
    @patch("armada.triggers.armada.ArmadaTrigger.client", new_callable=PropertyMock)
    async def test_execute(self, mock_client_fn, _, time_time):
        time_time.side_effect = self._time_side_effect

        test_cases = [
            {
                "name": "Job Succeeds",
                "statuses": [JobState.RUNNING, JobState.SUCCEEDED],
                "expected_responses": [
                    TriggerEvent(
                        {
                            "status": "success",
                            "job_id": DEFAULT_JOB_ID,
                            "response": f"Job {DEFAULT_JOB_ID} succeeded",
                        }
                    )
                ],
            },
            {
                "name": "Job Failed",
                "statuses": [JobState.RUNNING, JobState.FAILED],
                "success": False,
                "expected_responses": [
                    TriggerEvent(
                        {
                            "status": "error",
                            "job_id": DEFAULT_JOB_ID,
                            "response": f"Job {DEFAULT_JOB_ID} did not succeed. "
                            f"Final status was FAILED",
                        }
                    )
                ],
            },
            {
                "name": "Job cancelled",
                "statuses": [JobState.RUNNING, JobState.CANCELLED],
                "success": False,
                "expected_responses": [
                    TriggerEvent(
                        {
                            "status": "error",
                            "job_id": DEFAULT_JOB_ID,
                            "response": f"Job {DEFAULT_JOB_ID} did not succeed."
                            f" Final status was CANCELLED",
                        }
                    )
                ],
            },
            {
                "name": "Job unacknowledged",
                "statuses": [JobState.UNKNOWN for _ in range(6)],
                "success": False,
                "expected_responses": [
                    TriggerEvent(
                        {
                            "status": "error",
                            "job_id": DEFAULT_JOB_ID,
                            "response": f"Job {DEFAULT_JOB_ID} not acknowledged wit"
                            f"hin timeout {DEFAULT_JOB_ACKNOWLEDGEMENT_TIMEOUT}.",
                        }
                    )
                ],
            },
            {
                "name": "Job preempted",
                "statuses": [JobState.RUNNING, JobState.PREEMPTED],
                "success": False,
                "expected_responses": [
                    TriggerEvent(
                        {
                            "status": "error",
                            "job_id": DEFAULT_JOB_ID,
                            "response": f"Job {DEFAULT_JOB_ID} did not succeed."
                            f" Final status was PREEMPTED",
                        }
                    )
                ],
            },
            {
                "name": "Job Succeeds but takes a lot of transitions",
                "statuses": [
                    JobState.SUBMITTED,
                    JobState.RUNNING,
                    JobState.RUNNING,
                    JobState.RUNNING,
                    JobState.RUNNING,
                    JobState.RUNNING,
                    JobState.SUCCEEDED,
                ],
                "success": True,
                "expected_responses": [
                    TriggerEvent(
                        {
                            "status": "success",
                            "job_id": DEFAULT_JOB_ID,
                            "response": f"Job {DEFAULT_JOB_ID} succeeded",
                        }
                    )
                ],
            },
        ]

        for test_case in test_cases:
            with self.subTest(test_case=test_case["name"]):
                trigger = ArmadaTrigger(
                    job_id=DEFAULT_JOB_ID,
                    channel_args=GrpcChannelArgs,
                    poll_interval=DEFAULT_POLLING_INTERVAL,
                    tracking_message="some tracking message",
                    job_acknowledgement_timeout=DEFAULT_JOB_ACKNOWLEDGEMENT_TIMEOUT,
                    job_request_namespace="default",
                )

                # Setup Mock Armada
                mock_client = AsyncMock()
                mock_client.get_job_status.side_effect = [
                    job_pb2.JobStatusResponse(job_states={DEFAULT_JOB_ID: x})
                    for x in test_case["statuses"]
                ]
                mock_client.cancel_jobs.return_value = submit_pb2.CancellationResult(
                    cancelled_ids=[DEFAULT_JOB_ID]
                )
                mock_client_fn.return_value = mock_client
                responses = [gen async for gen in trigger.run()]
                self.assertEqual(test_case["expected_responses"], responses)
                self.assertEqual(
                    len(test_case["statuses"]), mock_client.get_job_status.call_count
                )

    @patch("time.sleep", return_value=None)
    @patch("armada.triggers.armada.ArmadaTrigger.client", new_callable=PropertyMock)
    async def test_unacknowledged_results_in_job_cancel(self, mock_client_fn, _):
        trigger = ArmadaTrigger(
            job_id=DEFAULT_JOB_ID,
            channel_args=GrpcChannelArgs,
            poll_interval=DEFAULT_POLLING_INTERVAL,
            tracking_message="some tracking message",
            job_acknowledgement_timeout=-1,
            job_request_namespace="default",
        )

        #  Set up Mock Armada
        mock_client = AsyncMock()
        mock_client.cancel_jobs.return_value = submit_pb2.CancellationResult(
            cancelled_ids=[DEFAULT_JOB_ID]
        )
        mock_client_fn.return_value = mock_client
        mock_client.get_job_status.side_effect = [
            job_pb2.JobStatusResponse(job_states={DEFAULT_JOB_ID: x})
            for x in [JobState.UNKNOWN, JobState.UNKNOWN]
        ]
        [gen async for gen in trigger.run()]

        self.assertEqual(mock_client.cancel_jobs.call_count, 1)
