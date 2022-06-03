from concurrent import futures
from armada_client.armada import submit_pb2_grpc, submit_pb2, event_pb2, event_pb2_grpc
from armada_client.armada.submit_pb2_grpc import SubmitServicer
import grpc
from google.protobuf import empty_pb2


class SubmitService(submit_pb2_grpc.SubmitServicer):
    def CreateQueue(self, request, context):
        return empty_pb2.Empty()

    def DeleteQueue(self, request, context):
        return empty_pb2.Empty()

    def GetQueue(self, request, context):
        return submit_pb2.Queue(name="test")

    def SubmitJobs(self, request, context):
        return submit_pb2.JobSubmitResponse()

    def GetQueueInfo(self, request, context):
        return empty_pb2.Empty()


class EventService(event_pb2_grpc.EventServicer):
    def Watch(self, request, context):
        return event_pb2.EventMessage()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    submit_pb2_grpc.add_SubmitServicer_to_server(SubmitService(), server)
    event_pb2_grpc.add_EventServicer_to_server(EventService(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
