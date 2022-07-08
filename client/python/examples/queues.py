import base64

import grpc
from armada_client.client import ArmadaClient

# The python GRPC library requires authentication
#  data to be provided as an AuthMetadataPlugin.
# The username/password are colon-delimted and base64 encoded as per RFC 2617


class GrpcBasicAuth(grpc.AuthMetadataPlugin):
    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password
        super().__init__()

    def __call__(self, context, callback):
        b64encoded_auth = base64.b64encode(
            bytes(f"{self._username}:{self._password}", "utf-8")
        ).decode("ascii")
        callback((("authorization", f"basic {b64encoded_auth}"),), None)


def main():
    disable_ssl = None
    host = "localhost"
    port = "8080"
    username = "admin"
    password = "admin"
    queue_name = "test-queue"

    if disable_ssl:
        channel_credentials = grpc.local_channel_credentials()
    else:
        channel_credentials = grpc.ssl_channel_credentials()

    channel = grpc.secure_channel(
        f"{host}:{port}",
        grpc.composite_channel_credentials(
            channel_credentials,
            grpc.metadata_call_credentials(GrpcBasicAuth(username, password)),
        ),
    )

    client = ArmadaClient(channel)
    client.create_queue(name=queue_name, priority_factor=200)
    info = client.get_queue_info(name=queue_name)
    print(info)
    print("============")
    info = client.get_queue(name=queue_name)
    print(info)


if __name__ == "__main__":
    main()
