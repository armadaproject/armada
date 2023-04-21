from concurrent import futures

import grpc
import pytest
import pytest_asyncio

from server_mock import EventService, SubmitService
from job_service_mock import JobService

from armada_client.armada import event_pb2_grpc, submit_pb2_grpc, submit_pb2
from armada_client.asyncio_client import ArmadaAsyncIOClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)

from armada.jobservice import jobservice_pb2_grpc


from armada.operators.armada_deferrable import ArmadaSubmitJobTrigger
from armada.operators.jobservice_asyncio import JobServiceAsyncIOClient


@pytest.fixture
def job_service_server_mock():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    jobservice_pb2_grpc.add_JobServiceServicer_to_server(JobService(), server)
    server.add_insecure_port("[::]:50099")
    server.start()
    yield
    server.stop(False)


@pytest.fixture
def armada_server_mock():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    submit_pb2_grpc.add_SubmitServicer_to_server(SubmitService(), server)
    event_pb2_grpc.add_EventServicer_to_server(EventService(), server)
    server.add_insecure_port("[::]:50100")
    server.start()
    yield
    server.stop(False)


@pytest_asyncio.fixture(scope="function")
async def js_aio_client(armada_server_mock):
    channel = grpc.aio.insecure_channel(
        target="127.0.0.1:50099",
        options={
            "grpc.keepalive_time_ms": 30000,
        }.items(),
    )
    await channel.channel_ready()
    assert channel.get_state(True) == grpc.ChannelConnectivity.READY

    return JobServiceAsyncIOClient(channel)


@pytest_asyncio.fixture(scope="function")
async def aio_client(armada_server_mock):
    channel = grpc.aio.insecure_channel(
        target="127.0.0.1:50100",
        options={
            "grpc.keepalive_time_ms": 30000,
        }.items(),
    )
    await channel.channel_ready()
    assert channel.get_state(True) == grpc.ChannelConnectivity.READY

    return ArmadaAsyncIOClient(channel)


@pytest.mark.asyncio
async def test_submit_job_trigger(
    armada_server_mock, job_service_server_mock, aio_client
):
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

    trigger = ArmadaSubmitJobTrigger(
        armada_channel_args={"target": "127.0.0.1:50100"},
        job_service_channel_args={"target": "127.0.0.1:50099"},
        run_id="test_run_id",
        armada_queue="test_queue",
        job_request_items=[request_item],
    )

    async for event in trigger.run():
        assert event.payload["job"].job_response_items[0].job_id == "job-1"
