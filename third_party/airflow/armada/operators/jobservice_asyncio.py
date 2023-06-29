from armada.jobservice import jobservice_pb2_grpc, jobservice_pb2

import grpc

from google.protobuf import empty_pb2

import tenacity


class JobServiceAsyncIOClient:
    """
    The JobService AsyncIO Client

    AsyncIO implementation of gRPC stubs from JobService

    :param channel: AsyncIO gRPC channel used for authentication. See
                    https://grpc.github.io/grpc/python/grpc_asyncio.html
                    for more information.
    :return: A job service client instance
    """

    def __init__(self, channel: grpc.aio.Channel) -> None:
        self.job_stub = jobservice_pb2_grpc.JobServiceStub(channel)

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True,
    )
    async def get_job_status(
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
        response = await self.job_stub.GetJobStatus(job_service_request)
        return response

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True,
    )
    async def health(self) -> jobservice_pb2.HealthCheckResponse:
        """Health Check for GRPC Request"""
        response = await self.job_stub.Health(request=empty_pb2.Empty())
        return response
