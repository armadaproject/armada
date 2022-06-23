from concurrent.futures import ThreadPoolExecutor
import os
from typing import Optional
from armada_client.armada import (
    jobcache_pb2,
    jobcache_pb2_grpc,
)


class JobCacheClint:
    """
    Client for accessing Armada over gRPC.

    :param channel: gRPC channel used for authentication. See
                    https://grpc.github.io/grpc/python/grpc.html
                    for more information.
    :param max_workers: number of cores for thread pools, if unset, defaults
                        to number of CPUs
    :return: an Armada client instance
    """

    def __init__(self, channel, max_workers: Optional[int] = None):
        self.executor = ThreadPoolExecutor(max_workers=max_workers or os.cpu_count())

        self.job_cache_stub = jobcache_pb2_grpc.JobCache(channel)

    def get_job_status(self, queue: str, job_set_id: str, job_id: str):
        """Get event stream for a job set.

        Uses the GetJobSetEvents rpc to get a stream of events relating
        to the provided job_set_id.

        :param queue: The name of the queue
        :param job_set_id: The name of the job set (a grouping of jobs)
        :param from_message_id: The from message id.
        :return: A job events stream for the job_set_id provided.
        """
        jsr = jobcache_pb2.JobCacheRequest(
            queue=queue,
            job_set_id=job_set_id,
            job_id=job_id,
        )
        return self.job_cache_stub.GetJobStatus(jsr)
