from armada_client.armada import (
    event_pb2,
    event_pb2_grpc,
    queue_pb2,
    queue_pb2_grpc,
    usage_pb2,
    usage_pb2_grpc,
    submit_pb2,
    submit_pb2_grpc,
)
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)
import grpc

no_auth_client = ArmadaClient(host='127.0.0.1', port=50051,
                              channel=grpc.insecure_channel(target=f"127.0.0.1:50051"))


def test_create_queue():
    no_auth_client.create_queue(name='test', priority_factor=1)
