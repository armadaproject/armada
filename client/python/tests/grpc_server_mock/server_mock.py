from asyncio import futures
from armada_client.armada.client import submit_pb2_grpc, submit_pb2
from armada_client.armada.client.submit_pb2_grpc import SubmitServicer
import grpc

class Greeter(submit_pb2_grpc.SubmitServicer):
    def CreateQueue(self, request, context):
        return ""

    def DeleteQueue(self, request, context):
        return super().DeleteQueue(request, context)

    def GetQueue(self, request, context):
        return submit_pb2.Queue(name='test', priority=1)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    submit_pb2_grpc.add_SubmitServicer_to_server(
        SubmitServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()