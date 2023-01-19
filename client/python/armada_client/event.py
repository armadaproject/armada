import typing

from armada_client.typings import EventType, OneOfJobEvent
from armada_client.armada import event_pb2


class Event:
    """
    Represents a gRPC proto event

    Definition can be found at:
    https://github.com/armadaproject/armada/blob/master/pkg/api/event.proto#L284

    :param event: The gRPC proto event
    """

    def __init__(
        self,
        event: event_pb2.EventStreamMessage,
    ):
        msg_type = event.message.WhichOneof("events")

        # check if msg_type is None
        if msg_type is None:
            raise ValueError("Event message type is None")

        message = getattr(event.message, msg_type)

        self.type: EventType = EventType(msg_type)
        self.message: OneOfJobEvent = typing.cast(OneOfJobEvent, message)
        self.id: str = event.id

    def __repr__(self):
        return (
            f"Event ID: {self.id}\n"
            f"Message Type {self.type} \n"
            f"Job ID: {self.message.job_id} \n"
            f"Job Set ID: {self.message.job_set_id} \n"
            f"Queue: {self.message.queue} \n"
        )
