import typing

from armada_client.typings import EventType, OneOfJobEvent
from armada_client.armada import event_pb2


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
        self.message: OneOfJobEvent = typing.cast(OneOfJobEvent, message)
        self.id: int = event.id

    def __repr__(self):
        return (
            f"Event ID: {self.id}\n"
            f"Message Type {self.type} \n"
            f"Job ID: {self.message.job_id} \n"
            f"Job Set ID: {self.message.job_set_id} \n"
            f"Queue: {self.message.queue} \n"
        )
