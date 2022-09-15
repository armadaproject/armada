import json
import os
import requests
from armada_client.client import ArmadaClient
import grpc

"""
This is an example of how you can use OIDC and GRPC with a ClientCredential flow.
"""

class GrpcAuth(grpc.AuthMetadataPlugin):
    def __init__(self, key):
        self._key = key

    def __call__(self, context, callback):
        callback((('authorization', self._key),), None)


def get_jwt():
    client_id = os.environ.get("OIDC_CLIENT_ID")
    client_secret = os.environ.get("OIDC_CLIENT_SECRET")
    oidc_provider = os.environ.get("OIDC_PROVIDER_URL")
    params = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }

    response = requests.post(
        f'{oidc_provider}', data=params)

    if response.status_code != 200:
        raise ValueError("Error accessing the API token via OAuth")
    return response.json()["access_token"]


def get_grpc_channel(jwt):
    channel = grpc.secure_channel(
        "localhost:50051",
        grpc.composite_channel_credentials(
            grpc.local_channel_credentials(),
            grpc.metadata_call_credentials(
                GrpcAuth('Bearer '+jwt)
            ),
        )
    )

    return channel


armada_client = ArmadaClient(channel=get_grpc_channel(get_jwt()))

queue = armada_client.create_queue_request(name="test-1", priority_factor=1.0)
armada_client.get_queue(name='test-1')
