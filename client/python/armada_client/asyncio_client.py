"""
Armada Python GRPC Client

For the api definitions:
https://armadaproject.io/api
"""

from datetime import timedelta
import logging
from typing import Dict, List, Optional, AsyncIterator

import grpc
from google.protobuf import empty_pb2

from armada_client.armada import (
    event_pb2,
    event_pb2_grpc,
    submit_pb2,
    submit_pb2_grpc,
    health_pb2,
    job_pb2,
    job_pb2_grpc,
)
from armada_client.event import Event
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.permissions import Permissions
from armada_client.typings import JobState
from armada_client.iterators import AsyncTimeoutIterator, IteratorTimeoutException


class _AsyncResilientArmadaEventStream(AsyncIterator[event_pb2.EventStreamMessage]):
    def __init__(
        self,
        *,
        queue: str,
        job_set_id: str,
        from_message_id: Optional[str] = None,
        event_stub: event_pb2_grpc.EventStub,
        event_timeout: timedelta,
    ):
        self._queue = queue
        self._job_set_id = job_set_id
        self._last_message_id = from_message_id or ""
        self._stream = None
        self._cancelled = False
        self._event_stub = event_stub
        self._event_timeout = event_timeout
        self._timeout_iterator = None

    def __aiter__(self) -> AsyncIterator[event_pb2.EventStreamMessage]:
        return self

    async def __anext__(self) -> event_pb2.EventStreamMessage:
        while True:
            if self._cancelled:
                raise StopAsyncIteration()
            if self._timeout_iterator is None:
                self._timeout_iterator = self._re_connect()
            try:
                # we can't use anext here, as it requires python 3.10+
                message = await self._timeout_iterator.__anext__()
                self._last_message_id = message.id
                return message
            except IteratorTimeoutException:
                self._timeout_iterator = None

    def _re_connect(self):
        self._close_connection()
        jsr = event_pb2.JobSetRequest(
            queue=self._queue,
            id=self._job_set_id,
            from_message_id=self._last_message_id,
            watch=True,
            errorIfMissing=True,
        )
        self._stream = self._event_stub.GetJobSetEvents(jsr)
        return AsyncTimeoutIterator(self._stream, timeout=self._event_timeout)

    def _close_connection(self):
        if self._stream is not None:
            self._stream.cancel()
            self._stream = None

    def cancel(self):
        self._cancelled = True
        self._close_connection()


logger = logging.getLogger("armada_client.asyncio_client")


class ArmadaAsyncIOClient:
    """
    Client for accessing Armada over gRPC with AsyncIO.

    :param channel: AsyncIO gRPC channel used for authentication. See
                    https://grpc.github.io/grpc/python/grpc_asyncio.html
                    for more information.
    :return: an Armada client instance
    """

    def __init__(
        self,
        channel: grpc.aio.Channel,
        event_timeout: timedelta = timedelta(minutes=15),
    ) -> None:
        self.submit_stub = submit_pb2_grpc.SubmitStub(channel)
        self.event_stub = event_pb2_grpc.EventStub(channel)
        self.job_stub = job_pb2_grpc.JobsStub(channel)
        self.event_timeout = event_timeout

    async def get_job_events_stream(
        self,
        queue: str,
        job_set_id: str,
        from_message_id: Optional[str] = None,
    ) -> AsyncIterator[event_pb2.EventStreamMessage]:
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
        return _AsyncResilientArmadaEventStream(
            queue=queue,
            job_set_id=job_set_id,
            from_message_id=from_message_id,
            event_stub=self.event_stub,
            event_timeout=self.event_timeout,
        )

    @staticmethod
    def unmarshal_event_response(event: event_pb2.EventStreamMessage) -> Event:
        """
        Unmarshal an event response from the gRPC server.

        :param event: The event response from the gRPC server.
        :return: An Event object.
        """

        return Event(event)

    async def submit_health(self) -> health_pb2.HealthCheckResponse:
        """
        Health check for Submit Service.
        :return: A HealthCheckResponse object.
        """
        response = await self.submit_stub.Health(request=empty_pb2.Empty())
        return response

    async def event_health(self) -> health_pb2.HealthCheckResponse:
        """
        Health check for Event Service.
        :return: A HealthCheckResponse object.
        """
        response = await self.event_stub.Health(request=empty_pb2.Empty())
        return response

    async def submit_jobs(
        self, queue: str, job_set_id: str, job_request_items
    ) -> AsyncIterator[submit_pb2.JobSubmitResponse]:
        """Submit an armada job.

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
        response = await self.submit_stub.SubmitJobs(request)
        return response

    async def get_job_status(self, job_ids: List[str]) -> job_pb2.JobStatusResponse:
        """
        Asynchronously retrieves the status of a list of jobs from Armada.

        :param job_ids: A list of unique job identifiers.
        :type job_ids: List[str]

        :returns: The response from the server containing the job status.
        :rtype: JobStatusResponse
        """
        req = job_pb2.JobStatusRequest(job_ids=job_ids)
        resp = await self.job_stub.GetJobStatus(req)
        return resp

    async def get_job_status_by_external_job_uri(
        self, queue: str, job_set_id: str, external_job_uri: str
    ) -> job_pb2.JobDetailsResponse:
        """
        Retrieves the status of a job based on externalJobUri annotation.

        :param queue: The name of the queue
        :param job_set_id: The name of the job set (a grouping of jobs)
        :param external_job_uri: externalJobUri annotation value

        :returns: The response from the server containing the job status.
        :rtype: JobStatusResponse
        """
        req = job_pb2.JobStatusUsingExternalJobUriRequest(
            queue=queue, jobset=job_set_id, external_job_uri=external_job_uri
        )
        resp = await self.job_stub.GetJobStatusUsingExternalJobUri(req)
        return resp

    async def get_job_errors(self, job_ids: List[str]) -> job_pb2.JobErrorsResponse:
        """
        Retrieves termination reason from query api.

        :param job_ids: A list of unique job identifiers.
        :type job_ids: List[str]

        :returns: The response from the server containing the job errors.
        :rtype: JobErrorsResponse
        """
        req = job_pb2.JobErrorsRequest(job_ids=job_ids)
        resp = await self.job_stub.GetJobErrors(req)
        return resp

    async def get_job_details(self, job_ids: List[str]) -> job_pb2.JobDetailsResponse:
        """
        Asynchronously retrieves the details of a job from Armada.

        :param job_ids: A list of unique job identifiers.
        :type job_ids: List[str]

        :returns: The Armada job details response.
        """
        req = job_pb2.JobDetailsRequest(job_ids=job_ids, expand_job_run=True)
        resp = await self.job_stub.GetJobDetails(req)
        return resp

    async def get_job_run_details(
        self, run_ids: List[str]
    ) -> job_pb2.JobRunDetailsResponse:
        """
        Asynchronously retrieves the details of a job run from Armada.

        :param run_ids: A list of unique job run identifiers.
        :type run_ids: List[str]

        :returns: The Armada run details response.
        """
        req = job_pb2.JobRunDetailsRequest(run_ids=run_ids)
        resp = await self.job_stub.GetJobRunDetails(req)
        return resp

    async def cancel_jobs(
        self,
        queue: str,
        job_set_id: str,
        job_id: Optional[str] = None,
    ) -> submit_pb2.CancellationResult:
        """Cancel jobs in a given queue.

        Uses the CancelJobs RPC to cancel jobs.

        :param queue: The name of the queue
        :param job_set_id: The name of the job set id
        :param job_id: The name of the job id (optional), if empty - cancel all jobs
        :return: A CancellationResult object.
        """
        if not queue or not job_set_id:
            raise ValueError("Both queue and job_set_id must be provided.")

        if job_id and queue and job_set_id:
            request = submit_pb2.JobCancelRequest(
                queue=queue, job_set_id=job_set_id, job_id=job_id
            )
            return await self.submit_stub.CancelJobs(request)
        else:
            logger.warning(
                "cancelling all jobs within a jobset via cancel_jobs is deprecated. "
                "Use cancel_jobset instead."
            )
            return await self.cancel_jobset(queue, job_set_id, [])  # type: ignore

    async def cancel_jobset(
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
            states=[
                submit_pb2.JobState.ValueType(state.value) for state in filter_states
            ]
        )
        request = submit_pb2.JobSetCancelRequest(
            queue=queue, job_set_id=job_set_id, filter=job_filter
        )
        response = await self.submit_stub.CancelJobSet(request)
        return response

    async def reprioritize_jobs(
        self,
        new_priority: float,
        job_ids: Optional[List[str]],
        job_set_id: str,
        queue: str,
    ) -> submit_pb2.JobReprioritizeResponse:
        """Reprioritize jobs with new_priority value.

        Uses ReprioritizeJobs RPC to set a new priority on a list of jobs
        or job set (if job_ids are set to None or empty).

        :param new_priority: The new priority value for the jobs
        :param job_ids: A list of job ids to change priority of
        :param job_set_id: A job set id including jobs to change priority of
        :param queue: The queue the jobs are in
        :return: JobReprioritizeResponse object. It is a map of strings.
        """
        if not queue or not job_set_id:
            raise ValueError("Both queue and job_set_id must be provided.")

        if job_ids:
            request = submit_pb2.JobReprioritizeRequest(
                queue=queue,
                job_set_id=job_set_id,
                job_ids=job_ids,
                new_priority=new_priority,
            )

        else:
            request = submit_pb2.JobReprioritizeRequest(
                queue=queue,
                job_set_id=job_set_id,
                new_priority=new_priority,
            )

        return await self.submit_stub.ReprioritizeJobs(request)

    async def preempt_jobs(
        self,
        queue: str,
        job_set_id: str,
        job_id: str,
    ) -> empty_pb2.Empty:
        """Preempt jobs in a given queue.

        Uses the PreemptJobs RPC to preempt jobs.

        :param queue: The name of the queue
        :param job_set_id: The name of the job set id
        :param job_id: The id the job
        :return: An empty response.
        """
        if not queue or not job_set_id or not job_id:
            raise ValueError("All of queue, job_set_id and job_id must be provided.")

        request = submit_pb2.JobPreemptRequest(
            queue=queue, job_set_id=job_set_id, job_ids=[job_id]
        )
        response = await self.submit_stub.PreemptJobs(request)
        return response

    async def create_queue(self, queue: submit_pb2.Queue) -> empty_pb2.Empty:
        """
        Uses the CreateQueue RPC to create a queue.

        :param queue: A queue to create.
        """

        response = await self.submit_stub.CreateQueue(queue)
        return response

    async def update_queue(self, queue: submit_pb2.Queue) -> empty_pb2.Empty:
        """
        Uses the UpdateQueue RPC to update a queue.

        :param queue: A queue to update.
        """

        response = await self.submit_stub.UpdateQueue(queue)
        return response

    async def create_queues(
        self, queues: List[submit_pb2.Queue]
    ) -> submit_pb2.BatchQueueCreateResponse:
        """
        Uses the CreateQueues RPC to create a list of queues.

        :param queues: A list of queues to create.
        """

        queue_list = submit_pb2.QueueList(queues=queues)
        response = await self.submit_stub.CreateQueues(queue_list)
        return response

    async def update_queues(
        self, queues: List[submit_pb2.Queue]
    ) -> submit_pb2.BatchQueueUpdateResponse:
        """
        Uses the UpdateQueues RPC to update a list of queues.

        :param queues: A list of queues to update.
        """

        queue_list = submit_pb2.QueueList(queues=queues)
        response = await self.submit_stub.UpdateQueues(queue_list)
        return response

    async def delete_queue(self, name: str) -> None:
        """Delete an empty queue by name.

        Uses the DeleteQueue RPC to delete the queue.

        :param name: The name of an empty queue
        :return: None
        """
        request = submit_pb2.QueueDeleteRequest(name=name)
        await self.submit_stub.DeleteQueue(request)

    async def get_queue(self, name: str) -> submit_pb2.Queue:
        """Get the queue by name.

        Uses the GetQueue RPC to get the queue.

        :param name: The name of the queue
        :return: A queue object. See the api definition.
        """
        request = submit_pb2.QueueGetRequest(name=name)
        response = await self.submit_stub.GetQueue(request)
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

        if priority_factor is None:
            priority_factor = 1.0

        queue_permissions: Optional[List[submit_pb2.Queue.Permissions]] = (
            [p.to_grpc() for p in permissions] if permissions else None
        )

        return submit_pb2.Queue(
            name=name,
            priority_factor=priority_factor,
            user_owners=user_owners,
            group_owners=group_owners,
            resource_limits=resource_limits,
            permissions=queue_permissions,
        )

    @staticmethod
    def create_job_request_item(
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

        # Set defaults for namespace and client_id
        if namespace is None:
            namespace = ""

        if client_id is None:
            client_id = ""

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
