import time
import uuid
from armada_client.generated_client import (
    event_pb2,
    event_pb2_grpc,
    queue_pb2,
    queue_pb2_grpc,
    usage_pb2,
    usage_pb2_grpc,
    submit_pb2,
    submit_pb2_grpc,
)
from armada_client.client import ArmadaClient, AuthData, AuthMethod
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)


class NoAuthTest:
    def __init__(self, host, port):
        # TODO: generalize this so tests can be run with a variety of auth schemas

        basic_auth_data = AuthData(
            AuthMethod.Anonymous
        )
        self.client = ArmadaClient(host, port, basic_auth_data, disable_ssl=True)

def test_no_auth_create_and_delete_queue():
    tester = NoAuthTest("127.0.0.1", "50051")
    tester.client.create_queue('test', priority_factor=200)
    queue = tester.client.get_queue('test')
    assert queue.name =='test'
    assert queue.priority_factor == 200.0
    tester.client.delete_queue('test')
