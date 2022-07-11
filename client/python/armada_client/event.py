from typing import Tuple

from armada_client.armada import event_pb2


class Message:
    """
    Type Hints for event stream
    """

    def __init__(self, message):
        self.message = message


class Event:
    """
    Represents a gRPC proto event

    Definition can be found at:
    https://github.com/G-Research/armada/blob/master/pkg/api/event.proto#L284

    :param event: The gRPC proto event stream message
    """

    def __init__(self, event: event_pb2.EventStreamMessage):
        self.original = event
        self.type, self.message = self.get_message()

    def get_message(self) -> Tuple[str, Message]:
        msg_type = self.original.message.WhichOneof("events")
        message = getattr(self.original.message, msg_type)
        return msg_type, message
