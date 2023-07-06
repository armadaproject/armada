import grpc

from armada.jobservice import jobservice_pb2, jobservice_pb2_grpc


# TODO - Make this a bit smarter, so we can hit at least one full
# loop in search_for_job_complete.
def mock_dummy_mapper_terminal(request):
    if request.job_id == "test_failed":
        return jobservice_pb2.JobServiceResponse(
            state=jobservice_pb2.JobServiceResponse.FAILED, error="Test Error"
        )
    if request.job_id == "test_succeeded":
        return jobservice_pb2.JobServiceResponse(
            state=jobservice_pb2.JobServiceResponse.SUCCEEDED
        )
    if request.job_id == "test_cancelled":
        return jobservice_pb2.JobServiceResponse(
            state=jobservice_pb2.JobServiceResponse.CANCELLED
        )
    return jobservice_pb2.JobServiceResponse(
        state=jobservice_pb2.JobServiceResponse.JOB_ID_NOT_FOUND
    )


class JobService(jobservice_pb2_grpc.JobServiceServicer):
    def GetJobStatus(self, request, context):
        return mock_dummy_mapper_terminal(request)

    def Health(self, request, context):
        return jobservice_pb2.HealthCheckResponse(
            status=jobservice_pb2.HealthCheckResponse.SERVING
        )


class JobServiceOccaisonalError(jobservice_pb2_grpc.JobServiceServicer):
    def __init__(self):
        self.get_job_status_count = 0
        self.health_count = 0

    def GetJobStatus(self, request, context):
        self.get_job_status_count += 1
        if self.get_job_status_count % 3 == 0:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Injected error")
            raise Exception("Injected error")

        if self.get_job_status_count < 5:
            return jobservice_pb2.JobServiceResponse(
                state=jobservice_pb2.JobServiceResponse.RUNNING
            )
        return jobservice_pb2.JobServiceResponse(
            state=jobservice_pb2.JobServiceResponse.SUCCEEDED
        )

    def Health(self, request, context):
        self.health_count += 1
        if self.health_count % 3 == 0:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Injected error")
            raise Exception("Injected error")

        return jobservice_pb2.HealthCheckResponse(
            status=jobservice_pb2.HealthCheckResponse.SERVING
        )
