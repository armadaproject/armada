import datetime
from typing import Optional


from armada_client.armada import (
    binoculars_pb2,
    binoculars_pb2_grpc,
)

from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1


class BinocularsClient:
    """
    Client for accessing Armada's Binoculars service over gRPC.

    :param channel: gRPC channel used for authentication. See
                      https://grpc.github.io/grpc/python/grpc.html
                      for more information.
    :return: an Binoculars client instance
    """

    def __init__(self, channel):
        self.binoculars_stub = binoculars_pb2_grpc.BinocularsStub(channel)

    def logs(
        self,
        job_id: str,
        pod_namespace: str,
        since_time: datetime.datetime,
        pod_number: Optional[int] = 0,
        log_options: Optional[core_v1.PodLogOptions] = None,
    ):
        log_request = binoculars_pb2.LogRequest(
            job_id=job_id,
            pod_number=pod_number,
            pod_namespace=pod_namespace,
            since_time=since_time.isoformat(),
            log_options=log_options,
        )
        return self.binoculars_stub.Logs(log_request)

    def cordon(self, node_name: str):
        cordon_request = binoculars_pb2.CordonRequest(node_name=node_name)
        return self.binoculars_stub.Cordon(cordon_request)
