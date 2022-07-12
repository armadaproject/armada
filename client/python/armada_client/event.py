import enum
import typing
from dataclasses import dataclass

import google.protobuf.timestamp_pb2 as timestamp_pb2

from armada_client.armada import event_pb2, queue_pb2
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as resource_pb2,
)


class EventType(enum.Enum):
    """
    Enum for the event states.
    """

    submitted = "submitted"
    queued = "queued"
    duplicate_found = "duplicate_found"
    leased = "leased"
    lease_returned = "lease_returned"
    pending = "pending"
    running = "running"
    unable_to_schedule = "unable_to_schedule"
    failed = "failed"
    succeeded = "succeeded"
    reprioritized = "reprioritized"
    cancelling = "cancelling"
    cancelled = "cancelled"
    terminated = "terminated"
    utilisation = "utilisation"
    ingress_info = "ingress_info"
    reprioritizing = "reprioritizing"
    updated = "updated"


@dataclass
class EventMessage:
    """
    EventMessage is the message type for the event stream.

    Based on event_pb2.EventMessage
    """

    job: queue_pb2.Job

    job_id: str
    job_set_id: str
    queue: str
    kubernetes_id: str
    pod_name: str
    pod_namespace: str
    cluster_id: str
    node_name: str
    reason: str
    requestor: str

    created: timestamp_pb2.Timestamp

    pod_number: int
    new_priority: float

    container_statuses: typing.Iterable[event_pb2.ContainerStatus]

    exit_codes: typing.Mapping[str, int]
    ingress_addresses: typing.Mapping[int, str]
    MaxResourcesForPeriod: typing.Mapping[str, resource_pb2.Quantity]
    total_cumulative_usage: typing.Mapping[str, resource_pb2.Quantity]


class Event:
    """
    Represents a gRPC proto event

    Definition can be found at:
    https://github.com/G-Research/armada/blob/master/pkg/api/event.proto#L284

    :param event: The gRPC proto event
    """

    def __init__(
        self,
        event: event_pb2.EventStreamMessage,
    ):
        msg_type = event.message.WhichOneof("events")
        message = getattr(event.message, msg_type)

        self.type: EventType = EventType(msg_type)
        self.message: EventMessage = typing.cast(EventMessage, message)
        self.id: int = event.id

    def __repr__(self):
        return (
            f"Event ID: {self.id}\n"
            f"Message Type {self.type} \n"
            f"Job ID: {self.message.job_id} \n"
            f"Job Set ID: {self.message.job_set_id} \n"
            f"Queue: {self.message.queue} \n"
        )
