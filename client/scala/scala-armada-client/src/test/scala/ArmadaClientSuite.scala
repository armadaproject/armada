package io.armadaproject.armada

import io.armadaproject.armada.ArmadaClient
import api.submit.{SubmitGrpc, CancellationResult, Queue, BatchQueueCreateResponse,
  StreamingQueueMessage, JobReprioritizeResponse, JobSubmitResponse,
  BatchQueueUpdateResponse, JobSubmitResponseItem, JobSubmitRequestItem,
  JobState, JobSetCancelRequest, JobCancelRequest, QueueDeleteRequest,
  QueueGetRequest, StreamingQueueGetRequest, JobPreemptRequest,
  JobReprioritizeRequest, JobSubmitRequest, QueueList}
import api.job.{JobRunState, JobsGrpc, JobErrorsResponse, JobDetailsRequest,
  JobRunDetailsResponse, JobDetailsResponse,
  JobStatusUsingExternalJobUriRequest, JobStatusResponse,
  JobErrorsRequest, JobRunDetailsRequest, JobStatusRequest}
import com.google.protobuf.empty.Empty
import api.health.HealthCheckResponse
import api.event.{EventGrpc, EventStreamMessage, JobSetRequest, WatchRequest}
import io.grpc.stub.StreamObserver
import io.grpc.{Server, ServerBuilder}

import scala.concurrent.Future

private class EventMockServer extends EventGrpc.Event {
  override def health(empty: Empty): scala.concurrent.Future[HealthCheckResponse] = {
    Future.successful(HealthCheckResponse(HealthCheckResponse.ServingStatus.SERVING))
  }

  override def getJobSetEvents(request: JobSetRequest,
      responseObserver: io.grpc.stub.StreamObserver[EventStreamMessage]): Unit = {
    // TODO: fill-in
  }

  override def watch(request: WatchRequest, responseObserver: io.grpc.stub.StreamObserver[EventStreamMessage]): Unit = {
    // TODO: fill-in
  }
}

private class SubmitMockServer extends SubmitGrpc.Submit {
  def cancelJobSet(request: JobSetCancelRequest): scala.concurrent.Future[com.google.protobuf.empty.Empty] = {
    Future.successful(new Empty)
  }

  def cancelJobs(request: JobCancelRequest): scala.concurrent.Future[CancellationResult] = {
    Future.successful(new CancellationResult)
  }

  def createQueue(request: Queue): scala.concurrent.Future[com.google.protobuf.empty.Empty] = {
    Future.successful(new Empty)
  }

  def createQueues(request: QueueList): scala.concurrent.Future[BatchQueueCreateResponse] = {
    Future.successful(new BatchQueueCreateResponse)
  }

  def deleteQueue(request: QueueDeleteRequest): scala.concurrent.Future[com.google.protobuf.empty.Empty] = {
    Future.successful(new Empty)
  }
  def getQueue(request: QueueGetRequest): scala.concurrent.Future[Queue] = {
    Future.successful(new Queue)
  }

  def getQueues(request: StreamingQueueGetRequest, responseObserver: io.grpc.stub.StreamObserver[StreamingQueueMessage]): Unit = {
    Future.successful(new StreamingQueueMessage)
  }

  def health(request: com.google.protobuf.empty.Empty): scala.concurrent.Future[HealthCheckResponse] = {
    Future.successful(HealthCheckResponse(HealthCheckResponse.ServingStatus.SERVING))
  }

  def preemptJobs(request: JobPreemptRequest): scala.concurrent.Future[com.google.protobuf.empty.Empty] = {
    Future.successful(new Empty)
  }

  def reprioritizeJobs(request: JobReprioritizeRequest): scala.concurrent.Future[JobReprioritizeResponse] = {
    Future.successful(new JobReprioritizeResponse)
  }

  def submitJobs(request: JobSubmitRequest): scala.concurrent.Future[JobSubmitResponse] = {
    Future.successful((new JobSubmitResponse(List(JobSubmitResponseItem("fakeJobId")))))
  }

  def updateQueue(request: Queue): scala.concurrent.Future[com.google.protobuf.empty.Empty] = {
    Future.successful(new Empty)
  }

  def updateQueues(request: QueueList): scala.concurrent.Future[BatchQueueUpdateResponse] = {
    Future.successful(new BatchQueueUpdateResponse)
  }

}

private class JobsMockServer extends JobsGrpc.Jobs {
  def getJobDetails(request: JobDetailsRequest): scala.concurrent.Future[JobDetailsResponse] = {
    Future.successful(new JobDetailsResponse)
  }

  def getJobErrors(request: JobErrorsRequest): scala.concurrent.Future[JobErrorsResponse] = {
    Future.successful(new JobErrorsResponse)
  }

  def getJobRunDetails(request: JobRunDetailsRequest): scala.concurrent.Future[JobRunDetailsResponse] = {
    Future.successful(new JobRunDetailsResponse)
  }

  def getJobStatus(request: JobStatusRequest): scala.concurrent.Future[JobStatusResponse] = {
    var response = new JobStatusResponse(Map("fakeJobId" -> JobState.RUNNING))
    Future.successful(response)
  }

  def getJobStatusUsingExternalJobUri(request: JobStatusUsingExternalJobUriRequest): scala.concurrent.Future[JobStatusResponse] = {
    Future.successful(new JobStatusResponse)
  }
}

// For more information on writing tests, see
// https://scalameta.org/munit/docs/getting-started.html
class ArmadaClientSuite extends munit.FunSuite {
  val testPort = 12345
  val mockEventServer = new Fixture[Server]("Event GRPC Mock Server") {
    private var server: Server = null
    def apply() = server
    override def beforeAll(): Unit = {
      import scala.concurrent.ExecutionContext
      server = ServerBuilder
        .forPort(testPort)
        .addService(EventGrpc.bindService(new EventMockServer, ExecutionContext.global))
        .addService(SubmitGrpc.bindService(new SubmitMockServer, ExecutionContext.global))
        .addService(JobsGrpc.bindService(new JobsMockServer, ExecutionContext.global))
        .build()
        .start()
    }
    override def afterAll(): Unit = {
      server.shutdown()
    }
  }

  override def munitFixtures = List(mockEventServer)

  test("ArmadaClient.EventHealth()") {
    val ac = new ArmadaClient(ArmadaClient.GetChannel("localhost", testPort))
    val status = ac.EventHealth()
    assertEquals(status, HealthCheckResponse.ServingStatus.SERVING)
  }

  test("ArmadaClient.SubmitHealth()") {
    val ac = new ArmadaClient(ArmadaClient.GetChannel("localhost", testPort))
    val status = ac.SubmitHealth()
    assertEquals(status, HealthCheckResponse.ServingStatus.SERVING)
  }

  test("ArmadaClient.SubmitJobs()") {
    val ac = new ArmadaClient(ArmadaClient.GetChannel("localhost", testPort))
    val response = ac.SubmitJobs("testQueue", "testJobSetId", List(new JobSubmitRequestItem()))
    assertEquals(response.jobResponseItems(0), JobSubmitResponseItem("fakeJobId"))
  }

  test("ArmadaClient.GetJobStatus()") {
    val ac = new ArmadaClient(ArmadaClient.GetChannel("localhost", testPort))
    val response = ac.GetJobStatus("fakeJobId")
    assert(response.jobStates("fakeJobId").isRunning)
  }
}
