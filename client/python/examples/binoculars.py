"""Example script utiltizing JobLogClient."""

import os

from armada_client.log_client import JobLogClient


DISABLE_SSL = os.environ.get("DISABLE_SSL", True)
HOST = os.environ.get("BINOCULARS_SERVER", "localhost")
PORT = os.environ.get("BINOCULARS_PORT", "50053")
JOB_ID = os.environ.get("JOB_ID")


def main():
    """Demonstrate basic use of JobLogClient."""
    url = f"{HOST}:{PORT}"
    client = JobLogClient(url, JOB_ID, DISABLE_SSL)

    log_lines = client.logs()

    for line in log_lines:
        print(line.line)


if __name__ == "__main__":
    main()
