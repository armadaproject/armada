"""
Armada Python GRPC Client

For the api definitions:
https://armadaproject.io/api
"""

import os
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Generator, List, Optional

from google.protobuf import empty_pb2

from armada_client.armada import (
    event_pb2,
    event_pb2_grpc,
    submit_pb2,
    submit_pb2_grpc,
    usage_pb2_grpc,
    health_pb2,
)
from armada_client.event import Event
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.permissions import Permissions
from armada_client.typings import JobState


class ArmadaClient:
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

        self.submit_stub = submit_pb2_grpc.SubmitStub(channel)
        self.event_stub = event_pb2_grpc.EventStub(channel)
        self.usage_stub = usage_pb2_grpc.UsageStub(channel)

    def get_job_events_stream(
        self,
        queue: str,
        job_set_id: str,
        from_message_id: Optional[str] = None,
    ) -> Generator[event_pb2.EventMessage, None, None]:
        """Get event stream for a job set.

        Uses the GetJobSetEvents rpc to get a stream of events relating
        to the provided job_set_id.

        Usage:

        .. code-block:: python

            events = client.get_job_events_stream(...)
            for event in events:
                event = client.unmarshal_event_response(event)
                print(event)

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

    @staticmethod
    def unmarshal_event_response(event: event_pb2.EventStreamMessage) -> Event:
        """
        Unmarshal an event response from the gRPC server.

        :param event: The event response from the gRPC server.
        :return: An Event object.
        """

        return Event(event)

    def submit_health(self) -> health_pb2.HealthCheckResponse:
        """
        Health check for Submit Service.
        :return: A HealthCheckResponse object.
        """
        return self.submit_stub.Health(request=empty_pb2.Empty())

    def event_health(self) -> health_pb2.HealthCheckResponse:
        """
        Health check for Event Service.
        :return: A HealthCheckResponse object.
        """
        return self.event_stub.Health(request=empty_pb2.Empty())

    def submit_jobs(
        self, queue: str, job_set_id: str, job_request_items
    ) -> submit_pb2.JobSubmitResponse:
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
    ) -> submit_pb2.JobCancelRequest:
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

    def cancel_jobset(
        self,
        queue: str,
        job_set_id: str,
        filter_states: List[JobState],
    ) -> empty_pb2.Empty:
        """Cancel jobs in a given queue.

        Uses the CancelJobSet RPC to cancel jobs.
        A filter is used to only cancel jobs in certain states.

        :param queue: The name of the queue
        :param job_set_id: An array of JobSubmitRequestItems.
        :param filter_states: A list of states to filter by.
        :return: An empty response.
        """

        job_filter = submit_pb2.JobSetFilter(
            states=[state.value for state in filter_states]
        )
        request = submit_pb2.JobSetCancelRequest(
            queue=queue, job_set_id=job_set_id, filter=job_filter
        )
        response = self.submit_stub.CancelJobSet(request)
        return response

    def reprioritize_jobs(
        self,
        new_priority: float,
        job_ids: Optional[List[str]] = None,
        job_set_id: Optional[str] = None,
        queue: Optional[str] = None,
    ) -> submit_pb2.JobReprioritizeResponse:
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

    def create_queue(self, queue: submit_pb2.Queue) -> empty_pb2.Empty:
        """
        Uses the CreateQueue RPC to create a queue.

        :param queue: A queue to create.
        """

        response = self.submit_stub.CreateQueue(queue)
        return response

    def update_queue(self, queue: submit_pb2.Queue) -> empty_pb2.Empty:
        """
        Uses the UpdateQueue RPC to update a queue.

        :param queue: A queue to update.
        """

        response = self.submit_stub.UpdateQueue(queue)
        return response

    def create_queues(
        self, queues: List[submit_pb2.Queue]
    ) -> submit_pb2.BatchQueueCreateResponse:
        """
        Uses the CreateQueues RPC to create a list of queues.

        :param queues: A list of queues to create.
        """

        queue_list = submit_pb2.QueueList(queues=queues)
        response = self.submit_stub.CreateQueues(queue_list)
        return response

    def update_queues(
        self, queues: List[submit_pb2.Queue]
    ) -> submit_pb2.BatchQueueUpdateResponse:
        """
        Uses the UpdateQueues RPC to update a list of queues.

        :param queues: A list of queues to update.
        """

        queue_list = submit_pb2.QueueList(queues=queues)
        response = self.submit_stub.UpdateQueues(queue_list)
        return response

    def delete_queue(self, name: str) -> None:
        """Delete an empty queue by name.

        Uses the DeleteQueue RPC to delete the queue.

        :param name: The name of an empty queue
        :return: None
        """
        request = submit_pb2.QueueDeleteRequest(name=name)
        self.submit_stub.DeleteQueue(request)

    def get_queue(self, name: str) -> submit_pb2.Queue:
        """Get the queue by name.

        Uses the GetQueue RPC to get the queue.

        :param name: The name of the queue
        :return: A queue object. See the api definition.
        """
        request = submit_pb2.QueueGetRequest(name=name)
        response = self.submit_stub.GetQueue(request)
        return response

    def get_queue_info(self, name: str) -> submit_pb2.QueueInfo:
        """Get the queue info by name.

        Uses the GetQueueInfo RPC to get queue info.

        :param name: The name of the queue
        :return: A queue info object.  See the api definition.
        """
        request = submit_pb2.QueueInfoRequest(name=name)
        response = self.submit_stub.GetQueueInfo(request)
        return response

    @staticmethod
    def unwatch_events(event_stream) -> None:
        """Closes gRPC event streams

        Closes the provided event_stream.queue

        :param event_stream: a gRPC event stream
        :return: nothing
        """
        event_stream.cancel()

    def create_queue_request(
        self,
        name: str,
        priority_factor: Optional[float],
        user_owners: Optional[List[str]] = None,
        group_owners: Optional[List[str]] = None,
        resource_limits: Optional[Dict[str, float]] = None,
        permissions: Optional[List[Permissions]] = None,
    ) -> submit_pb2.Queue:
        """
        Create a queue request object.

        :param name: The name of the queue
        :param priority_factor: The priority factor for the queue
        :param user_owners: The user owners for the queue
        :param group_owners: The group owners for the queue
        :param resource_limits: The resource limits for the queue
        :param permissions: The permissions for the queue
        :return: A queue request object.
        """

        permissions = [p.to_grpc() for p in permissions] if permissions else None

        return submit_pb2.Queue(
            name=name,
            priority_factor=priority_factor,
            user_owners=user_owners,
            group_owners=group_owners,
            resource_limits=resource_limits,
            permissions=permissions,
        )

    def create_job_request_item(
        self,
        priority: float = 1.0,
        pod_spec: Optional[core_v1.PodSpec] = None,
        pod_specs: Optional[List[core_v1.PodSpec]] = None,
        namespace: Optional[str] = None,
        client_id: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
        annotations: Optional[Dict[str, str]] = None,
        required_node_labels: Optional[Dict[str, str]] = None,
        ingress: Optional[List[submit_pb2.IngressConfig]] = None,
        services: Optional[List[submit_pb2.ServiceConfig]] = None,
    ) -> submit_pb2.JobSubmitRequestItem:
        """Create a job request.

        :param priority: The priority of the job

        :param  pod_spec: The k8s pod spec of the job
        :type pod_spec:
            Optional[armada_client.k8s.io.api.core.v1.generated_pb2.PodSpec]

        :param pod_specs: List of k8s pod specs of the job
        :type pod_specs:
            Optional[List[armada_client.k8s.io.api.core.v1.generated_pb2.PodSpec]]

        :param namespace: The namespace of the job
        :param client_id: The client id of the job
        :param labels: The labels of the job
        :param annotations: The annotations of the job
        :param required_node_labels: The required node labels of the job
        :param ingress: The ingress of the job
        :param services: The services of the job
        :return: A job item request object. See the api definition.
        """

        if pod_spec and pod_specs:
            raise ValueError("Only one of pod_spec and pod_specs can be specified")

        return submit_pb2.JobSubmitRequestItem(
            priority=priority,
            pod_spec=pod_spec,
            pod_specs=pod_specs,
            namespace=namespace,
            client_id=client_id,
            labels=labels,
            annotations=annotations,
            required_node_labels=required_node_labels,
            ingress=ingress,
            services=services,
        )
