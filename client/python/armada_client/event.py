import typing
from dataclasses import dataclass
from enum import Enum

from armada_client.armada import event_pb2


class EventType(Enum):
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

    job_id: str
    job_set_id: str
    queue: str
    kubernetes_id: str
    pod_name: str
    pod_namespace: str
    cluster_id: str
    created: str
    node_name: str
    reason: str

    pod_number: int

    # TODO
    container_statuses = "container_statuses"
    exit_codes = "exit_codes"


class Event:
    """
    Represents a gRPC proto event

    Definition can be found at:
    https://github.com/G-Research/armada/blob/master/pkg/api/event.proto#L284

    :param event: The gRPC proto event
    :param message: The message to be parsed
    :param msg_type: The type of message
    """

    def __init__(
        self,
        event: event_pb2.EventStreamMessage,
        message: event_pb2.EventMessage,
        msg_type: str,
    ):
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
