from google.protobuf import empty_pb2
from armada_client.armada import submit_pb2_grpc, submit_pb2, event_pb2, event_pb2_grpc


class SubmitService(submit_pb2_grpc.SubmitServicer):
    def CreateQueue(self, request, context):
        return empty_pb2.Empty()

    def DeleteQueue(self, request, context):
        return empty_pb2.Empty()

    def GetQueue(self, request, context):
        return submit_pb2.Queue(name=request.name)

    def SubmitJobs(self, request, context):
        return submit_pb2.JobSubmitResponse()

    def GetQueueInfo(self, request, context):
        return submit_pb2.QueueInfo()

    def CancelJobs(self, request, context):
        return submit_pb2.CancellationResult()

    def ReprioritizeJobs(self, request, context):
        return submit_pb2.JobReprioritizeResponse()

    def UpdateQueue(self, request, context):
        return empty_pb2.Empty()

    def CreateQueues(self, request, context):
        return submit_pb2.BatchQueueCreateResponse()

    def UpdateQueues(self, request, context):
        return submit_pb2.BatchQueueUpdateResponse()


class EventService(event_pb2_grpc.EventServicer):
    def GetJobSetEvents(self, request, context):

        events = [event_pb2.EventStreamMessage()]

        for event in events:
            yield event
