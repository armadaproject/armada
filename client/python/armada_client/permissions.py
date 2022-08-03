from typing import NamedTuple, List
from armada_client.armada import submit_pb2


class Subject(NamedTuple):
    """
    Subject is a NamedTuple that represents a subject in the permission system.
    """

    kind: str
    name: str

    def to_grpc(self) -> submit_pb2.Queue.Permissions.Subject:
        """
        Convert this Subject to a grpc Subject.
        """
        return submit_pb2.Queue.Permissions.Subject(kind=self.kind, name=self.name)


class Permissions:
    """
    Permissions including Subjects and Verbs

    .. code-block:: python

        permissions = Permissions(...)
        client = ArmadaClient(...)

        queue = client.create_queue(
            permissions=[permissions],
        )
    """

    def __init__(self, subjects: List[Subject], verbs: List[str]):
        self.subjects = subjects
        self.verbs = verbs

    def to_grpc(self) -> submit_pb2.Queue.Permissions:
        """
        Convert to grpc object
        """

        return submit_pb2.Queue.Permissions(
            subjects=[s.to_grpc() for s in self.subjects],
            verbs=self.verbs,
        )
