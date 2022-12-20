from google.protobuf import empty_pb2
from armada_client.armada import (
    submit_pb2_grpc,
    submit_pb2,
    event_pb2,
    event_pb2_grpc,
    health_pb2,
)


class SubmitService(submit_pb2_grpc.SubmitServicer):
    def CreateQueue(self, request, context):
        return empty_pb2.Empty()

    def DeleteQueue(self, request, context):
        return empty_pb2.Empty()

    def GetQueue(self, request, context):
        return submit_pb2.Queue(name=request.name)

    def SubmitJobs(self, request, context):
        return submit_pb2.JobSubmitResponse(
            job_response_items=[
                submit_pb2.JobSubmitResponseItem(
                    job_id="test",
                )
            ]
        )

    def GetQueueInfo(self, request, context):
        return submit_pb2.QueueInfo()

    def CancelJobs(self, request, context):
        return submit_pb2.CancellationResult(
            cancelled_ids=["test"],
        )

    def CancelJobSet(self, request, context):
        return empty_pb2.Empty()

    def ReprioritizeJobs(self, request, context):

        new_priority = request.new_priority

        return submit_pb2.JobReprioritizeResponse(
            reprioritization_results={"test", new_priority}
        )

    def UpdateQueue(self, request, context):
        return empty_pb2.Empty()

    def CreateQueues(self, request, context):
        return submit_pb2.BatchQueueCreateResponse(
            failed_queues=[
                submit_pb2.QueueCreateResponse(
                    queue=submit_pb2.Queue(name="test"),
                )
            ]
        )

    def UpdateQueues(self, request, context):
        return submit_pb2.BatchQueueUpdateResponse(
            failed_queues=[
                submit_pb2.QueueUpdateResponse(
                    queue=submit_pb2.Queue(name="test"),
                )
            ]
        )

    def Health(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.SERVING
        )


class EventService(event_pb2_grpc.EventServicer):
    def GetJobSetEvents(self, request, context):

        events = [event_pb2.EventStreamMessage()]

        for event in events:
            yield event

    def Health(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.SERVING
        )
