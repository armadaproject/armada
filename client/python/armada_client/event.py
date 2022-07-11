class Event:
    """
    Represents a gRPC proto event

    Definition can be found at:
    https://github.com/G-Research/armada/blob/master/pkg/api/event.proto#L284

    :param event: The gRPC proto event
    """

    def __init__(self, event):
        self.original = event
