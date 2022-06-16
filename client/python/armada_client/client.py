"""
Armada Python GRPC Client
"""
from concurrent.futures import ThreadPoolExecutor
import os
from typing import List, Optional
from armada_client.armada import (
    event_pb2,
    event_pb2_grpc,
    usage_pb2_grpc,
    submit_pb2_grpc,
    submit_pb2,
)


class ArmadaClient:
    """
    Client for accessing Armada over gRPC.

    :param channel: gRPC channel used for authentication. See
                    https://grpc.github.io/grpc/python/grpc.html
                    for more information.
    :param max_workers: number of cores for thread pools
    :return: an Armada client instance
    """

    def __init__(self, channel, max_workers: int = os.cpu_count()):
        self.executor = ThreadPoolExecutor(max_workers=max_workers or 1)

        self.submit_stub = submit_pb2_grpc.SubmitStub(channel)
        self.event_stub = event_pb2_grpc.EventStub(channel)
        self.usage_stub = usage_pb2_grpc.UsageStub(channel)

    def get_job_events_stream(
        self,
        queue: str,
        job_set_id: str,
        from_message_id: Optional[str] = None,
    ):
        """Get event stream for a job set.

        Uses the GetJobSetEvents rpc to get a stream of events relating
        to the provided job_set_id.

        :param queue: The name of the queue
        :param job_set_id: The name of the job set (a grouping of jobs)
        :param from_message_id: The from message id.
        :return: A job events stream for the job_set_id provided.
        """
        jsr = event_pb2.JobSetRequest(
            queue=queue,
            id=job_set_id,
            from_message_id=from_message_id,
            watch=True,
            errorIfMissing=True,
        )
        return self.event_stub.GetJobSetEvents(jsr)

    def submit_jobs(self, queue: str, job_set_id: str, job_request_items):
        """Submit a armada job.

        Uses SubmitJobs RPC to submit a job.

        :param queue: The name of the queue
        :param job_set_id: The name of the job set (a grouping of jobs)
        :param job_request_items: List[JobSubmitRequestItem]
                                  An array of JobSubmitRequestItems.
        :return: A JobSubmitResponse object.
        """
        request = submit_pb2.JobSubmitRequest(
            queue=queue, job_set_id=job_set_id, job_request_items=job_request_items
        )
        response = self.submit_stub.SubmitJobs(request)
        return response

    def cancel_jobs(
        self,
        queue: Optional[str] = None,
        job_id: Optional[str] = None,
        job_set_id: Optional[str] = None,
    ):
        """Cancel jobs in a given queue.

        Uses the CancelJobs RPC to cancel jobs. Either job_id or
        job_set_id is required.

        :param queue: The name of the queue
        :param job_id: The name of the job id (this or job_set_id required)
        :param job_set_id: An array of JobSubmitRequestItems. (this or job_id required)
        :return: A JobSubmitResponse object.
        """
        request = submit_pb2.JobCancelRequest(
            queue=queue, job_id=job_id, job_set_id=job_set_id
        )
        response = self.submit_stub.CancelJobs(request)
        return response

    def reprioritize_jobs(
        self,
        new_priority: float,
        job_ids: Optional[List[str]] = None,
        job_set_id: Optional[str] = None,
        queue: Optional[str] = None,
    ):
        """Reprioritize jobs with new_priority value.

        Uses ReprioritizeJobs RPC to set a new priority on a list of jobs
        or job set.

        :param new_priority: The new priority value for the jobs
        :param job_ids: A list of job ids to change priority of
        :param job_set_id: A job set id including jobs to change priority of
        :param queue: The queue the jobs are in
        :return: ReprioritizeJobsResponse object. It is a map of strings.
        """
        request = submit_pb2.JobReprioritizeRequest(
            job_ids=job_ids,
            job_set_id=job_set_id,
            queue=queue,
            new_priority=new_priority,
        )
        response = self.submit_stub.ReprioritizeJobs(request)
        return response

    def create_queue(self, name: str, **queue_params):
        """Create the queue by name.

        Uses the CreateQueue RPC to create a queue.

        :param name: The name of the queue
        :param queue_params: Queue Object
        :return: A queue object per the Armada api definition.
        """
        request = submit_pb2.Queue(name=name, **queue_params)
        response = self.submit_stub.CreateQueue(request)
        return response

    def update_queue(self, name: str, **queue_params) -> None:
        """Update the queue of name with values in queue_params

        Uses UpdateQueue RPC to update the parameters on the queue.

        :param name: The name of the queue
        :param queue_params: Queue Object
        :return: None
        """
        request = submit_pb2.Queue(name=name, **queue_params)
        self.submit_stub.UpdateQueue(request)

    def delete_queue(self, name: str) -> None:
        """Delete an empty queue by name.

        Uses the DeleteQueue RPC to delete the queue.

        :param name: The name of an empty queue
        :return: None
        """
        request = submit_pb2.QueueDeleteRequest(name=name)
        self.submit_stub.DeleteQueue(request)

    def get_queue(self, name: str):
        """Get the queue by name.

        Uses the GetQueue RPC to get the queue.

        :param name: The name of the queue
        :return: A queue object. See the api definition.
        """
        request = submit_pb2.QueueGetRequest(name=name)
        response = self.submit_stub.GetQueue(request)
        return response

    def get_queue_info(self, name: str):
        """Get the queue info by name.

        Uses the GetQueueInfo RPC to get queue info.

        :param name: The name of the queue
        :return: A queue info object.  See the api definition.
        """
        request = submit_pb2.QueueInfoRequest(name=name)
        response = self.submit_stub.GetQueueInfo(request)
        return response


def unwatch_events(event_stream) -> None:
    """Closes gRPC event streams

    Closes the provided event_stream.queue

    :param event_stream: a gRPC event stream
    :return: nothing
    """
    event_stream.cancel()
