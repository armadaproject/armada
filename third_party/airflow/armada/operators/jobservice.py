from concurrent.futures import ThreadPoolExecutor
import os
from typing import Optional
from armada.jobservice import jobservice_pb2_grpc, jobservice_pb2

from google.protobuf import empty_pb2


class JobServiceClient:
    """
    The JobService Client

    Implementation of gRPC stubs from JobService

    :param channel: gRPC channel used for authentication. See
                    https://grpc.github.io/grpc/python/grpc.html
                    for more information.
    :param max_workers: number of cores for thread pools, if unset, defaults
                        to number of CPUs
    :return: a job service client instance
    """

    def __init__(self, channel, max_workers: Optional[int] = None):
        self.executor = ThreadPoolExecutor(max_workers=max_workers or os.cpu_count())

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
