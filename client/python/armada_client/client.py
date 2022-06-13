"""
Armada Python GRPC Client
"""
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import os
from typing import Callable, List, Optional
import grpc
from armada_client.armada import (
    event_pb2,
    event_pb2_grpc,
    usage_pb2_grpc,
    submit_pb2_grpc,
    submit_pb2,
)


class ArmadaClient:
    """
    The Armada Client
    Implementation of gRPC stubs from events, queues and submit

    Attributes:
        channel: gRPC channel
        max_workers: number of cores for thread pools
    gRPC channels is for authentication.
    See https://grpc.github.io/grpc/python/grpc.html
    """

    def __init__(self, channel, max_workers=os.cpu_count()):

        self.executor = ThreadPoolExecutor(max_workers=max_workers or 1)

        self.submit_stub = submit_pb2_grpc.SubmitStub(channel)
        self.event_stub = event_pb2_grpc.EventStub(channel)
        self.usage_stub = usage_pb2_grpc.UsageStub(channel)

    def submit_jobs(self, queue: str, job_set_id: str, job_request_items):
        """Submit a armada job.
        :param queue: str
            The name of the queue
        :param job_set_id: str
            The name of the job set (a grouping of jobs)
        :param job_request_items: List[JobSubmitRequestItem]
            An array of JobSubmitRequestItems.
            See the api definition in submit.proto for a definition.

        This calls the SubmitJob rpc call.
        :return: A JobSubmitResponse object.
        """
        request = submit_pb2.JobSubmitRequest(
            queue=queue, job_set_id=job_set_id, job_request_items=job_request_items
        )
        response = self.submit_stub.SubmitJobs(request)
        return response

    def cancel_jobs(self, queue=Optional[str], job_id=Optional[str], job_set_id=Optional[str]):
        """CancelJobs rpc call.  Cancel jobs in a given queue, job_set_id, job_id
        :param queue: str
            The name of the queue
        :param job_id: str
            The name of the job id 
        :param job_set_id: List[JobSubmitRequestItem]
            An array of JobSubmitRequestItems.
            See the api definition in submit.proto for a definition.

        This calls the SubmitJob rpc call.
        :return: A JobSubmitResponse object.
        """

        request = submit_pb2.JobCancelRequest(
            queue=queue, job_id=job_id, job_set_id=job_set_id
        )
        response = self.submit_stub.CancelJobs(request)
        return response

    def reprioritize_jobs(
        self, new_priority: float, job_ids=List[str], job_set_id = Optional[str], queue = Optional[str]
    ):
        """Reprioritize jobs with new_priority value.  
        Can be applied all jobs in a job_set_id or 
        applied to a list of jobs
        param: new_priority: float
            The new priority value for the jobs
        param: job_ids: A list of job ids
            Apply new_priority to list of jobs 
        param: job_set_id: str A job set id
        param: queue: str The queue 

        This calls the ReprioritizeJobs rpc call.
        :return: ReprioritizeJobsResponse object.  It is a map of strings.
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
        param: name: str
            The name of the queue
        param: queue_params: Queue Object
            The params is a queue object

        This calls the GetQueue rpc call.
        :return: A queue object.  See the api definition.
        """
        request = submit_pb2.Queue(name=name, **queue_params)
        response = self.submit_stub.CreateQueue(request)
        return response

    def update_queue(self, name: str, **queue_params) -> None:
        """Update the queue of name with values in queue_params
        param: name: str
            The name of the queue
        param: queue_params: Queue Object
            The params is a queue object

        This calls the UpdateQueue rpc call.
        :return: An empty object
        """
        request = submit_pb2.Queue(name=name, **queue_params)
        self.submit_stub.UpdateQueue(request)

    def delete_queue(self, name: str) -> None:
        """Delete a queue by name.
        param: name: str
            The name of the queue

        This calls the DeleteQueue rpc call.
        Only empty queues can be deleted.
        """
        request = submit_pb2.QueueDeleteRequest(name=name)
        response = self.submit_stub.DeleteQueue(request)

    def get_queue(self, name: str):
        """Get the queue by name.
        param: name: str
            The name of the queue

        This calls the GetQueue rpc call.
        :return: A queue object.  See the api definition.
        """
        request = submit_pb2.QueueGetRequest(name=name)
        response = self.submit_stub.GetQueue(request)
        return response

    def get_queue_info(self, name: str):
        """Get the queue info by name.
        param: name: str
            The name of the queue

        This calls the GetQueueInfo rpc call.
        : return: A queue info object.  See the api definition.
        """
        request = submit_pb2.QueueInfoRequest(name=name)
        response = self.submit_stub.GetQueueInfo(request)
        return response

    def watch_events(self, on_event:Callable, queue:str, job_set_id:str, from_message_id=Optional[str]):
        """Watch events .
        param: queue: str
            The name of the queue
        param: job_set_id: str
            The name of the job_set_id to watch
        param: on_event: Callable
            A function for handling streams 
        param: from_message_id: str
            TBD: What is this

        This calls the Watchevents rpc call.
        : return: A stream of potential messages from events.  See events.proto for a comprehensive list.
        """
        jsr = event_pb2.JobSetRequest(
            queue=queue,
            id=job_set_id,
            from_message_id=from_message_id,
            watch=True,
            errorIfMissing=True,
        )
        event_stream = self.event_stub.GetJobSetEvents(jsr)

        def event_counter(event_stream):
            try:
                for event in event_stream:
                    on_event(event)
            except (
                grpc._channel._MultiThreadedRendezvous
            ) as error:  # pylint: disable=protected-access
                if error.code() == grpc.StatusCode.CANCELLED:
                    pass
                # process cancelled status
                elif (
                    error.code() == grpc.StatusCode.UNAVAILABLE
                    and "Connection reset by peer" in error.details()
                ):
                    pass
                # process unavailable status
                else:
                    raise

        event_function = partial(event_counter, event_stream=event_stream)
        self.executor.submit(event_function)
        return event_stream


def unwatch_events(event_stream):
    """Grpc way to cancel a stream"""
    event_stream.cancel()
