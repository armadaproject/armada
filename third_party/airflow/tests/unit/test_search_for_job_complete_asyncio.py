from concurrent import futures

import grpc
import pytest
import pytest_asyncio

from job_service_mock import JobService

from armada.operators.jobservice_asyncio import JobServiceAsyncIOClient
from armada.operators.utils import JobState, search_for_job_complete_async
from armada.jobservice import jobservice_pb2_grpc


@pytest.fixture
def server_mock():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    jobservice_pb2_grpc.add_JobServiceServicer_to_server(JobService(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    yield
    server.stop(False)


@pytest_asyncio.fixture(scope="function")
async def js_aio_client(server_mock):
    channel = grpc.aio.insecure_channel(
        target="127.0.0.1:50051",
        options={
            "grpc.keepalive_time_ms": 30000,
        }.items(),
    )
    await channel.channel_ready()
    assert channel.get_state(True) == grpc.ChannelConnectivity.READY

    return JobServiceAsyncIOClient(channel)


@pytest.mark.asyncio
async def test_failed_event(js_aio_client):
    job_complete = await search_for_job_complete_async(
        airflow_task_name="test",
        job_id="test_failed",
        armada_queue="test",
        job_set_id="test",
        job_service_client=js_aio_client,
        time_out_for_failure=5,
    )
    assert job_complete[0] == JobState.FAILED
    assert (
        job_complete[1]
        == "Armada test:test_failed failed\nfailed with reason Test Error"
    )


@pytest.mark.asyncio
async def test_successful_event(js_aio_client):
    job_complete = await search_for_job_complete_async(
        airflow_task_name="test",
        job_id="test_succeeded",
        armada_queue="test",
        job_set_id="test",
        job_service_client=js_aio_client,
        time_out_for_failure=5,
    )
    assert job_complete[0] == JobState.SUCCEEDED
    assert job_complete[1] == "Armada test:test_succeeded succeeded"


@pytest.mark.asyncio
async def test_cancelled_event(js_aio_client):
    job_complete = await search_for_job_complete_async(
        airflow_task_name="test",
        job_id="test_cancelled",
        armada_queue="test",
        job_set_id="test",
        job_service_client=js_aio_client,
        time_out_for_failure=5,
    )
    assert job_complete[0] == JobState.CANCELLED
    assert job_complete[1] == "Armada test:test_cancelled cancelled"


@pytest.mark.asyncio
async def test_job_id_not_found(js_aio_client):
    job_complete = await search_for_job_complete_async(
        airflow_task_name="test",
        job_id="id",
        armada_queue="test",
        job_set_id="test",
        time_out_for_failure=5,
        job_service_client=js_aio_client,
    )
    assert job_complete[0] == JobState.JOB_ID_NOT_FOUND
    assert (
        job_complete[1] == "Armada test:id could not find a job id and\nhit a timeout"
    )
