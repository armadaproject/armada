import os

import grpc
from armada_client.binoculars_client import BinocularsClient


def main():
    if DISABLE_SSL:
        channel = grpc.insecure_channel(f"{HOST}:{PORT}")
    else:
        channel_credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(
            f"{HOST}:{PORT}",
            channel_credentials,
        )

    client = BinocularsClient(channel)

    log_response = client.logs(
        JOB_ID,
        "default",
        "",
    )

    for line in log_response.log:
        print(line)


if __name__ == "__main__":
    DISABLE_SSL = os.environ.get("DISABLE_SSL", True)
    HOST = os.environ.get("BINOCULARS_SERVER", "localhost")
    PORT = os.environ.get("BINOCULARS_PORT", "50053")
    JOB_ID = os.environ.get("JOB_ID")

    main()
