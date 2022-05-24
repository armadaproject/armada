from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)

import grpc


def test_no_auth_create_and_delete_queue():
    tester = ArmadaClient(
        "127.0.0.1", "50051", grpc.insecure_channel(target="127.0.0.1:50051")
    )
    tester.create_queue("test", priority_factor=200)
    queue = tester.get_queue("test")
    assert queue.name == "test"
    assert queue.priority_factor == 200.0
    tester.delete_queue("test")
