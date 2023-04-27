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
