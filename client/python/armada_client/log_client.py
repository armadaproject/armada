from dataclasses import dataclass
from typing import Optional

from armada_client.internal.binoculars_client import new_binoculars_client


@dataclass
class LogLine:
    """Represents a single line from a log."""

    line: str
    timestamp: str


class JobLogClient:
    """
    Client for retrieving logs for a given job.

    :param url: The url to use for retreiving logs.
    :param job_id:  The ID of the job.
    :return: A JobLogClient instance.
    """

    def __init__(self, url: str, job_id: str, disable_ssl: bool = False):
        self.job_id = job_id
        self.url = url
        self._channel, self._concrete_client = new_binoculars_client(
            self.url, disable_ssl
        )

    def logs(self, since_time: Optional[str] = ""):
        """Retrieve logs for the job associated with this client.

        :param since_time: Logs will be retrieved starting at the time
          specified in this str. Must conform to RFC3339 date time format.

        :return: A list of LogLine objects.
        """
        return [
            LogLine(line.line, line.timestamp)
            for line in self._concrete_client.logs(self.job_id, since_time).log
        ]
