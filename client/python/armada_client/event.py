import typing
from enum import Enum

import google.protobuf.timestamp_pb2 as timestamp_pb2

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


class _MsgAttribute(Enum):
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
    reason = "reason"

    # TODO
    container_statuses = "container_statuses"
    exit_codes = "exit_codes"


class Message:
    """
    Represents a gRPC proto event message

    Definition can be found at:
    https://github.com/G-Research/armada/blob/master/pkg/api/event.proto#L284

    :param message: The message to be parsed
    :param msg_type: The type of message
    """

    def __init__(self, message: event_pb2.EventStreamMessage, msg_type: str):
        self.msg_type: EventType = EventType(msg_type)
        self.original: event_pb2.EventStreamMessage = message

        self.job_id: str = self._get_string_object(_MsgAttribute.job_id)
        self.job_set_id: str = self._get_string_object(_MsgAttribute.job_set_id)
        self.queue: str = self._get_string_object(_MsgAttribute.queue)
        self.kubernetes_id: str = self._get_string_object(_MsgAttribute.kubernetes_id)
        self.pod_name: str = self._get_string_object(_MsgAttribute.pod_name)
        self.pod_namespace: str = self._get_string_object(_MsgAttribute.pod_namespace)
        self.cluster_id: str = self._get_string_object(_MsgAttribute.cluster_id)
        self.node_name: str = self._get_string_object(_MsgAttribute.node_name)

        self.created: timestamp_pb2.Timestamp = self._get_timestamp_object(
            _MsgAttribute.created
        )

        self.pod_number: int = self._get_int_object(_MsgAttribute.pod_number)

    def _get_int_object(self, attribute: _MsgAttribute) -> int:
        """
        Returns an int object from the message

        This is needed to stop MyPy erroring, as it complains about
        getattr accepting any return type.
        """

        int_object = getattr(self.original, attribute.name, None)
        int_object = typing.cast(int, int_object)

        return int_object

    def _get_string_object(self, attribute: _MsgAttribute) -> str:
        """
        Returns a string object from the message

        This is needed to stop MyPy erroring, as it complains about
        getattr accepting any return type.
        """

        string = getattr(self.original, attribute.name, None)
        string = typing.cast(str, string)

        return string

    def _get_timestamp_object(
        self, attribute: _MsgAttribute
    ) -> timestamp_pb2.Timestamp:
        """
        Returns a timestamp object from the message

        This is needed to stop MyPy erroring, as it complains about
        getattr accepting any return type.
        """

        timestamp = getattr(self.original, attribute.name, None)
        timestamp = typing.cast(timestamp_pb2.Timestamp, timestamp)

        return timestamp

    def __repr__(self):
        return (
            f"Message Type {self.msg_type} \n"
            f"Job ID: {self.job_id} \n"
            f"Job Set ID: {self.job_set_id} \n"
            f"Queue: {self.queue} \n"
        )
