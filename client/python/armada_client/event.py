import typing
from enum import Enum

import google.protobuf.timestamp_pb2 as timestamp_pb2

from armada_client.armada import event_pb2


class EventType(Enum):
    """
    Struct for the event states.
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


class MsgAttributes(Enum):
    """
    All Attributes for a Message
    """

    job_id = "job_id"
    job_set_id = "job_set_id"
    queue = "queue"
    kubernetes_id = "kubernetes_id"
    pod_name = "pod_name"
    pod_namespace = "pod_namespace"
    pod_number = "pod_number"
    cluster_id = "cluster_id"
    created = "created"
    node_name = "node_name"


class Message:
    """
    Represents a gRPC proto event message

    Definition can be found at:
    https://github.com/G-Research/armada/blob/master/pkg/api/event.proto#L284

    :param message: The message to be parsed
    :param msg_type: The type of message
    """

    def __init__(self, message: event_pb2.EventStreamMessage, msg_type: str):
        self.type: EventType = EventType(msg_type)
        self.original: event_pb2.EventStreamMessage = message

        self.job_id = getattr(message, MsgAttributes.job_id.name, None)
        self.job_set_id = getattr(message, MsgAttributes.job_set_id.name, None)
        self.queue = getattr(message, MsgAttributes.queue.name, None)
        self.kubernetes_id = getattr(message, MsgAttributes.kubernetes_id.name, None)
        self.pod_name = getattr(message, MsgAttributes.pod_name.name, None)
        self.pod_namespace = getattr(message, MsgAttributes.pod_namespace.name, None)
        self.pod_number = getattr(message, MsgAttributes.pod_number.name, None)
        self.cluster_id = getattr(message, MsgAttributes.cluster_id.name, None)
        self.created: timestamp_pb2.Timestamp = self.get_timestamp_object()
        self.node_name = getattr(message, MsgAttributes.node_name.name, None)

    def get_timestamp_object(self) -> timestamp_pb2.Timestamp:
        """
        Returns a timestamp object from the message

        This is needed to stop MyPy erroring, as it complains about
        getattr accepting any return type.
        """

        timestamp = getattr(self.original, MsgAttributes.cluster_id.name, None)
        timestamp = typing.cast(timestamp_pb2.Timestamp, timestamp)

        return timestamp

    def __repr__(self):
        return (
            f"Message Type {self.type} \n"
            f"Job ID: {self.job_id} \n"
            f"Job Set ID: {self.job_set_id} \n"
            f"Queue: {self.queue} \n"
        )
