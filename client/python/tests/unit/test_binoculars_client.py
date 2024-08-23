from concurrent import futures
import datetime

import grpc
import pytest

from google.protobuf import empty_pb2

from server_mock import BinocularsService

from armada_client.armada import binoculars_pb2_grpc
from armada_client.binoculars_client import BinocularsClient


@pytest.fixture(scope="session", autouse=True)
def binoculars_server_mock():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    binoculars_pb2_grpc.add_BinocularsServicer_to_server(BinocularsService(), server)
    server.add_insecure_port("[::]:4000")
    server.start()

    yield
    server.stop(False)


channel = grpc.insecure_channel(target="127.0.0.1:4000")
tester = BinocularsClient(
    grpc.insecure_channel(
        target="127.0.0.1:4000",
        options={
            "grpc.keepalive_time_ms": 30000,
        }.items(),
    )
)


def test_logs():
    resp = tester.logs("fake-job-id", "fake-namespace", datetime.datetime.now())
    assert len(resp.log) == 3


def test_cordon():
    result = tester.cordon("fake-node-name")
    assert result == empty_pb2.Empty()
