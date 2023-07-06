import json

from armada.jobservice import jobservice_pb2_grpc, jobservice_pb2

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
                            "maxAttempts": 5,
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
