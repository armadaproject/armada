from concurrent import futures

import grpc
import pytest
import pytest_asyncio

from server_mock import EventService, SubmitService

from armada_client.armada import event_pb2_grpc, submit_pb2_grpc, submit_pb2, health_pb2
from armada_client.asyncio_client import ArmadaAsyncIOClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)

from armada_client.permissions import Permissions, Subject
from armada_client.typings import JobState


@pytest.fixture
def server_mock():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    submit_pb2_grpc.add_SubmitServicer_to_server(SubmitService(), server)
    event_pb2_grpc.add_EventServicer_to_server(EventService(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    yield
    server.stop(False)


@pytest_asyncio.fixture(scope="function")
async def aio_client(server_mock):
    channel = grpc.aio.insecure_channel(
        target="127.0.0.1:50051",
        options={
            "grpc.keepalive_time_ms": 30000,
        }.items(),
    )
    await channel.channel_ready()
    assert channel.get_state(True) == grpc.ChannelConnectivity.READY

    return ArmadaAsyncIOClient(channel)


@pytest.mark.asyncio
async def test_submit_job(aio_client):
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name="Container1",
                image="index.docker.io/library/ubuntu:latest",
                args=["sleep", "10s"],
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

    labels = {
        "app": "test",
    }
    annotations = {
        "test": "test",
    }
    required_node_labels = {
        "test": "test",
    }

    ingress = submit_pb2.IngressConfig()
    services = submit_pb2.ServiceConfig()

    request_item = aio_client.create_job_request_item(
        priority=1,
        pod_spec=pod,
        namespace="test",
        client_id="test",
        labels=labels,
        annotations=annotations,
        required_node_labels=required_node_labels,
        ingress=[ingress],
        services=[services],
    )

    resp = await aio_client.submit_jobs(
        queue="test",
        job_set_id="test",
        job_request_items=[request_item],
    )

    assert resp.job_response_items[0].job_id == "job-1"


@pytest.mark.asyncio
async def test_create_queue(aio_client):
    queue = aio_client.create_queue_request(name="test", priority_factor=1)
    await aio_client.create_queue(queue)


@pytest.mark.asyncio
async def test_create_queue_full(aio_client):
    resource_limits = {
        "cpu": 0.2,
    }

    sub = Subject("Group", "group1")
    permissions = Permissions([sub], ["get", "post"])

    queue = aio_client.create_queue_request(
        name="test",
        priority_factor=1,
        user_owners=["test"],
        group_owners=["test"],
        resource_limits=resource_limits,
        permissions=[permissions],
    )

    await aio_client.create_queue(queue)


@pytest.mark.asyncio
async def test_create_queues(aio_client):
    queue = aio_client.create_queue_request(name="test", priority_factor=1)
    queue2 = aio_client.create_queue_request(name="test2", priority_factor=1)

    resp = await aio_client.create_queues([queue, queue2])

    assert len(resp.failed_queues) == 2


@pytest.mark.asyncio
async def test_create_queues_full(aio_client):
    resource_limits = {
        "cpu": 0.2,
    }

    sub = Subject("Group", "group1")
    permissions = Permissions([sub], ["get", "post"])

    queue = aio_client.create_queue_request(
        name="test",
        priority_factor=1,
        user_owners=["test"],
        group_owners=["test"],
        resource_limits=resource_limits,
        permissions=[permissions],
    )

    queue2 = aio_client.create_queue_request(name="test2", priority_factor=1)

    resp = await aio_client.create_queues([queue, queue2])

    assert len(resp.failed_queues) == 2


@pytest.mark.asyncio
async def test_get_queue(aio_client):
    queue = await aio_client.get_queue("test")
    assert queue.name == "test"


@pytest.mark.asyncio
async def test_delete_queue(aio_client):
    await aio_client.delete_queue("test")


@pytest.mark.asyncio
async def test_get_queue_info(aio_client):
    queue = await aio_client.get_queue_info(name="test")
    assert queue.name == "test"


@pytest.mark.asyncio
async def test_cancel_jobs(aio_client):
    await test_create_queue(aio_client)
    await test_submit_job(aio_client)

    # Test that the right combination of jobid or jobsetid and queue is used
    # also check that the Value error is raised
    with pytest.raises(ValueError):
        await aio_client.cancel_jobs(
            queue="test", job_id="job-1", job_set_id="job-set-1"
        )

    resp = await aio_client.cancel_jobs(job_id="job-1")

    assert resp.cancelled_ids[0] == "job-1"

    resp = await aio_client.cancel_jobs(queue="test", job_set_id="job-set-1")

    assert len(list(resp.cancelled_ids)) > 0


@pytest.mark.asyncio
async def test_cancel_jobset(aio_client):
    await test_create_queue(aio_client)
    await test_submit_job(aio_client)
    await aio_client.cancel_jobset(
        queue="test",
        job_set_id="job-set-1",
        filter_states=[JobState.RUNNING, JobState.PENDING],
    )


@pytest.mark.asyncio
async def test_update_queue(aio_client):
    queue = aio_client.create_queue_request(name="test", priority_factor=1)
    await aio_client.update_queue(queue)


@pytest.mark.asyncio
async def test_update_queue_full(aio_client):
    resource_limits = {
        "cpu": 0.2,
    }

    sub = Subject("Group", "group1")
    permissions = Permissions([sub], ["get", "post"])

    queue = aio_client.create_queue_request(
        name="test",
        priority_factor=1,
        user_owners=["test"],
        group_owners=["test"],
        resource_limits=resource_limits,
        permissions=[permissions],
    )
    await aio_client.update_queue(queue)


@pytest.mark.asyncio
async def test_update_queues(aio_client):
    queue = aio_client.create_queue_request(name="test", priority_factor=1)
    queue2 = aio_client.create_queue_request(name="test2", priority_factor=1)

    resp = await aio_client.update_queues([queue, queue2])

    assert len(resp.failed_queues) == 2


@pytest.mark.asyncio
async def test_update_queues_full(aio_client):
    resource_limits = {
        "cpu": 0.2,
    }

    sub = Subject("Group", "group1")
    permissions = Permissions([sub], ["get", "post"])

    queue = aio_client.create_queue_request(
        name="test",
        priority_factor=1,
        user_owners=["test"],
        group_owners=["test"],
        resource_limits=resource_limits,
        permissions=[permissions],
    )
    queue2 = aio_client.create_queue_request(name="test2", priority_factor=1)

    resp = await aio_client.update_queues([queue, queue2])

    assert len(resp.failed_queues) == 2


@pytest.mark.asyncio
async def test_reprioritize_jobs(aio_client):
    # Similar to test_cancel_jobs(), test that the right combination of jobid
    # or jobsetid and queue is used
    # also check that the Value error is raised

    with pytest.raises(ValueError):
        await aio_client.reprioritize_jobs(
            queue="test",
            job_ids=["job-1"],
            job_set_id="job-set-1",
            new_priority=1,
        )

    resp = await aio_client.reprioritize_jobs(
        job_ids=["job-1"],
        new_priority=1,
    )

    assert resp.reprioritization_results == {"job-1": "1.0"}

    resp = await aio_client.reprioritize_jobs(
        queue="test",
        job_set_id="job-set-1",
        new_priority=1,
    )

    assert resp.reprioritization_results == {"test/job-set-1": "1.0"}


@pytest.mark.asyncio
async def test_get_job_events_stream(aio_client):
    events = await aio_client.get_job_events_stream(
        queue="test", job_set_id="job-set-1"
    )

    async for _ in events:
        pass


@pytest.mark.asyncio
async def test_health_submit(aio_client):
    health = await aio_client.submit_health()
    assert health.SERVING == health_pb2.HealthCheckResponse.SERVING


@pytest.mark.asyncio
async def test_health_event(aio_client):
    health = await aio_client.event_health()
    assert health.SERVING == health_pb2.HealthCheckResponse.SERVING
