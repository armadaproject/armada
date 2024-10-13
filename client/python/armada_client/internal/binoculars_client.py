from typing import Optional

import grpc

from armada_client.armada import (
    binoculars_pb2,
    binoculars_pb2_grpc,
)

from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1


def new_binoculars_client(url: str, disable_ssl: bool = False):
    """Constructs and returns a new BinocularsClient object.

    :param url: A url specifying the gRPC binoculars endpoint in the format
    "host:port".

    :return: A new BinocularsClient object.
    """
    parts = url.split(":")
    if len(parts) != 2:
        raise ValueError(f"Could not parse url provided: {url}")

    host, port = parts[0], parts[1]
    if disable_ssl:
        channel = grpc.insecure_channel(f"{host}:{port}")
    else:
        channel_credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(
            f"{host}:{port}",
            channel_credentials,
        )

    client = BinocularsClient(channel)
    return (channel, client)


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
        since_time: str,
        pod_namespace: Optional[str] = "default",
        pod_number: Optional[int] = 0,
        log_options: Optional[core_v1.PodLogOptions] = core_v1.PodLogOptions(),
    ):
        """Retrieve logs for a specific Armada job.

        :param job_id: The ID of the job for which to retreieve logs.
        :param pod_namespace: The namespace of the pod/job.
        :param since_time: If the empty string, retrieves all available logs.
          Otherwise, retrieves logs emitted since given timestamp.
        :param pod_number: The zero-indexed pod number from which to retrieve
          logs. Defaults to zero.
        :param log_options: An optional Kubernetes PodLogOptions object.
        :return: A LogResponse object.
        """
        log_request = binoculars_pb2.LogRequest(
            job_id=job_id,
            pod_number=pod_number,
            pod_namespace=pod_namespace,
            since_time=since_time,
            log_options=log_options,
        )
        return self.binoculars_stub.Logs(log_request)

    def cordon(self, node_name: str):
        """Send a cordon request for a specific node.

        :param node_name: The name of the node.
        :return: Empty grpc object.
        """
        cordon_request = binoculars_pb2.CordonRequest(node_name=node_name)
        return self.binoculars_stub.Cordon(cordon_request)
