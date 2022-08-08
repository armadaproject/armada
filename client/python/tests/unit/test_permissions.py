from armada_client.permissions import Permissions, Subject

from armada_client.armada import submit_pb2


def test_subject():
    sub = Subject("Group", "group1")
    assert sub.to_grpc() == submit_pb2.Queue.Permissions.Subject(
        kind="Group", name="group1"
    )


def test_permissions():
    sub = Subject("Group", "group1")
    per = Permissions([sub], ["get", "post"])
    assert per.to_grpc() == submit_pb2.Queue.Permissions(
        subjects=[sub.to_grpc()], verbs=["get", "post"]
    )
