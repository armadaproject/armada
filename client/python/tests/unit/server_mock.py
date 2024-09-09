from google.protobuf import empty_pb2

from armada_client.armada import (
    submit_pb2_grpc,
    submit_pb2,
    event_pb2,
    event_pb2_grpc,
    health_pb2,
    job_pb2_grpc,
    job_pb2,
)
from armada_client.armada.job_pb2 import JobRunState
from armada_client.armada.submit_pb2 import JobState


class QueueService(submit_pb2_grpc.QueueServiceServicer):

    def CreateQueue(self, request, context):
        return empty_pb2.Empty()

    def DeleteQueue(self, request, context):
        return empty_pb2.Empty()

    def GetQueue(self, request, context):
        return submit_pb2.Queue(name=request.name)

    def GetQueueInfo(self, request, context):
        return submit_pb2.QueueInfo(name=request.name)

    def CreateQueues(self, request, context):
        return submit_pb2.BatchQueueCreateResponse(
            failed_queues=[
                submit_pb2.QueueCreateResponse(queue=submit_pb2.Queue(name=queue.name))
                for queue in request.queues
            ]
        )

    def UpdateQueues(self, request, context):
        return submit_pb2.BatchQueueUpdateResponse(
            failed_queues=[
                submit_pb2.QueueUpdateResponse(queue=submit_pb2.Queue(name=queue.name))
                for queue in request.queues
            ]
        )

    def UpdateQueue(self, request, context):
        return empty_pb2.Empty()


class SubmitService(submit_pb2_grpc.SubmitServicer):

    def SubmitJobs(self, request, context):
        # read job_ids from request.job_request_items
        job_ids = [f"job-{i}" for i in range(1, len(request.job_request_items) + 1)]

        job_response_items = [
            submit_pb2.JobSubmitResponseItem(job_id=job_id) for job_id in job_ids
        ]

        return submit_pb2.JobSubmitResponse(job_response_items=job_response_items)


    def CancelJobs(self, request, context):
        return submit_pb2.CancellationResult(
            cancelled_ids=["job-1"],
        )

    def CancelJobSet(self, request, context):
        return empty_pb2.Empty()

    def PreemptJobs(self, request, context):
        return empty_pb2.Empty()

    def ReprioritizeJobs(self, request, context):
        new_priority = request.new_priority
        if len(request.job_ids) > 0:
            job_id = request.job_ids[0]
            results = {
                f"{job_id}": new_priority,
            }

        else:
            queue = request.queue
            job_set_id = request.job_set_id

            results = {
                f"{queue}/{job_set_id}": new_priority,
            }

        # convert the result dict into a list of tuples
        # while also converting ints to strings

        results = [(k, str(v)) for k, v in results.items()]

        return submit_pb2.JobReprioritizeResponse(reprioritization_results=results)

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


class QueryAPIService(job_pb2_grpc.JobsServicer):
    DEFAULT_JOB_DETAILS = {
        "queue": "test_queue",
        "jobset": "test_jobset",
        "namespace": "test_namespace",
        "state": JobState.RUNNING,
        "cancel_reason": "",
        "latest_run_id": "0",
    }

    DEFAULT_JOB_RUN_DETAILS = {
        "job_id": "0",
        "cluster": "test_cluster",
        "node": "test_node",
        "state": JobRunState.RUN_STATE_RUNNING,
    }

    def GetJobStatus(self, request, context):
        return job_pb2.JobStatusResponse(
            job_states={job: JobState.RUNNING for job in request.job_ids}
        )

    def GetJobDetails(self, request, context):
        return job_pb2.JobDetailsResponse(
            job_details={
                job: job_pb2.JobDetails(
                    job_id=job, **QueryAPIService.DEFAULT_JOB_DETAILS
                )
                for job in request.job_ids
            }
        )

    def GetJobRunDetails(self, request, context):
        return job_pb2.JobRunDetailsResponse(
            job_run_details={
                run: job_pb2.JobRunDetails(
                    run_id=run, **QueryAPIService.DEFAULT_JOB_RUN_DETAILS
                )
                for run in request.run_ids
            }
        )
