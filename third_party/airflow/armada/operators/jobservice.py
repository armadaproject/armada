import json
from typing import List, Optional, Tuple

from armada.jobservice import jobservice_pb2_grpc, jobservice_pb2

import grpc
from google.protobuf import empty_pb2

default_jobservice_channel_options = [
    (
        "grpc.service_config",
        json.dumps(
            {
                "methodConfig": [
                    {
                        "name": [{"service": "jobservice.JobService"}],
                        "retryPolicy": {
                            "maxAttempts": 6 * 5,  # A little under 5 minutes.
                            "initialBackoff": "0.1s",
                            "maxBackoff": "10s",
                            "backoffMultiplier": 2,
                            "retryableStatusCodes": ["UNAVAILABLE"],
                        },
                    }
                ]
            }
        ),
    )
]


class JobServiceClient:
    """
    The JobService Client

    Implementation of gRPC stubs from JobService

    :param channel: gRPC channel used for authentication. See
                    https://grpc.github.io/grpc/python/grpc.html
                    for more information.
    :return: a job service client instance
    """

    def __init__(self, channel):
        self.job_stub = jobservice_pb2_grpc.JobServiceStub(channel)

    def get_job_status(
        self, queue: str, job_set_id: str, job_id: str
    ) -> jobservice_pb2.JobServiceResponse:
        """Get job status of a given job in a queue and job_set_id.

        Uses the GetJobStatus rpc to get a status of your job

        :param queue: The name of the queue
        :param job_set_id: The name of the job set (a grouping of jobs)
        :param job_id: The id of the job
        :return: A Job Service Request (State, Error)
        """
        job_service_request = jobservice_pb2.JobServiceRequest(
            queue=queue, job_set_id=job_set_id, job_id=job_id
        )
        return self.job_stub.GetJobStatus(job_service_request)

    def health(self) -> jobservice_pb2.HealthCheckResponse:
        """Health Check for GRPC Request"""
        return self.job_stub.Health(request=empty_pb2.Empty())


def get_retryable_job_service_client(
    target: str,
    credentials: Optional[grpc.ChannelCredentials],
    compression: Optional[grpc.Compression],
) -> JobServiceClient:
    """
    Get a JobServiceClient that has retry configured

    :param target: grpc channel target
    :param credentials: grpc channel credentials (if needed)
    :param compresion: grpc channel compression

    :return: A job service client instance
    """
    channel = None
    if credentials is None:
        channel = grpc.insecure_channel(
            target=target,
            options=default_jobservice_channel_options,
            compression=compression,
        )
    else:
        channel = grpc.secure_channel(
            target=target,
            credentials=credentials,
            options=default_jobservice_channel_options,
            compression=compression,
        )
    return JobServiceClient(channel)
