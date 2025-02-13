package io.armadaproject.armada

import io.armadaproject.armada.ArmadaClient
import api.submit.{SubmitGrpc, CancellationResult, Queue, BatchQueueCreateResponse,
  StreamingQueueMessage, Job, JobReprioritizeResponse, JobSubmitResponse,
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
import io.grpc.{Server, ServerBuilder, Status, StatusRuntimeException}
import jkugiya.ulid.ULID

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Future
import scala.util.Random
import javax.print.attribute.standard.JobPriority

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

private class SubmitMockServer(jobMap: ConcurrentHashMap[String, Job], queueMap: ConcurrentHashMap[String, Queue])
  extends SubmitGrpc.Submit {

  val ulidGen = ULID.getGenerator()

  def cancelJobSet(request: JobSetCancelRequest): scala.concurrent.Future[com.google.protobuf.empty.Empty] = {
    Future.successful(new Empty)
  }

  def cancelJobs(request: JobCancelRequest): scala.concurrent.Future[CancellationResult] = {
    Future.successful(new CancellationResult)
  }

  def createQueue(request: Queue): scala.concurrent.Future[com.google.protobuf.empty.Empty] = {
    queueMap.put(request.name, request)
    Future.successful(new Empty)
  }

  def createQueues(request: QueueList): scala.concurrent.Future[BatchQueueCreateResponse] = {
    request.queues.foreach { q => queueMap.put(q.name, q) }
    Future.successful(new BatchQueueCreateResponse)
  }

  def deleteQueue(request: QueueDeleteRequest): scala.concurrent.Future[com.google.protobuf.empty.Empty] = {
    queueMap.remove(request.name)
    Future.successful(new Empty)
  }

  def getQueue(request: QueueGetRequest): scala.concurrent.Future[Queue] = {
    val q = queueMap.get(request.name)
    if (q == null) {
      Future.failed(new StatusRuntimeException(Status.NOT_FOUND))
    } else {
      Future.successful(q)
    }
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
    val q = queueMap.get(request.queue)
    if (q == null) {
      // TODO RETURN ERROR THAT QUEUE DOES NOT EXIST
    }

    val jobId: String = ulidGen.base32().toLowerCase()
    val newJob: Job = new Job()

    jobMap.put(jobId, newJob)
    Future.successful((new JobSubmitResponse(List(JobSubmitResponseItem(jobId)))))
  }

  def updateQueue(request: Queue): scala.concurrent.Future[com.google.protobuf.empty.Empty] = {
    Future.successful(new Empty)
  }

  def updateQueues(request: QueueList): scala.concurrent.Future[BatchQueueUpdateResponse] = {
    Future.successful(new BatchQueueUpdateResponse)
  }
}

private class JobsMockServer(jobMap: ConcurrentHashMap[String, Job]) extends JobsGrpc.Jobs {
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
    val statusMap = collection.mutable.Map[String,JobState]() // jobID -> state
    val it = jobMap.keys.asIterator()

    while (it.hasNext()) statusMap.put(it.next(), JobState.RUNNING)

    Future.successful(new JobStatusResponse(statusMap.to(collection.immutable.Map)))
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

    private val jobMap: ConcurrentHashMap[String, Job] = new ConcurrentHashMap       // key is job id
    private val queueMap: ConcurrentHashMap[String, Queue] = new ConcurrentHashMap   // key is queue name

    override def beforeAll(): Unit = {
      import scala.concurrent.ExecutionContext
      server = ServerBuilder
        .forPort(testPort)
        .addService(EventGrpc.bindService(new EventMockServer, ExecutionContext.global))
        .addService(SubmitGrpc.bindService(new SubmitMockServer(jobMap, queueMap), ExecutionContext.global))
        .addService(JobsGrpc.bindService(new JobsMockServer(jobMap), ExecutionContext.global))
        .build()
        .start()
    }
    override def afterAll(): Unit = {
      server.shutdown()
    }
  }

  override def munitFixtures = List(mockEventServer)

  test("ArmadaClient.EventHealth()") {
    val ac = ArmadaClient("localhost", testPort)
    val status = ac.eventHealth()
    assertEquals(status, HealthCheckResponse.ServingStatus.SERVING)
  }

  test("ArmadaClient.SubmitHealth()") {
    val ac = ArmadaClient("localhost", testPort)
    val status = ac.submitHealth()
    assertEquals(status, HealthCheckResponse.ServingStatus.SERVING)
  }

  test("ArmadaClient.SubmitJobs()") {
    val ac = ArmadaClient("localhost", testPort)
    val response = ac.submitJobs("testQueue", "testJobSetId", List(new JobSubmitRequestItem()))
    assertEquals(1, response.jobResponseItems.length)
    assertEquals(26, response.jobResponseItems(0).jobId.length)
  }

  test("ArmadaClient.GetJobStatus()") {
    val ac = ArmadaClient("localhost", testPort)
    val newJob = ac.submitJobs("testQueue", "testJobSetId", List(new JobSubmitRequestItem()))

    val jobId = newJob.jobResponseItems(0).jobId
    val jobStatus = ac.getJobStatus(jobId)
    assert(jobStatus.jobStates(jobId).isRunning)
  }

  test("test queue existence, creation, deletion") {
    val ac = ArmadaClient("localhost", testPort)
    val qName = "test-queue-" + Random.alphanumeric.take(8).mkString
    var q: Queue = new Queue()

    // queue should not exist yet
    intercept[StatusRuntimeException] {
      q = ac.getQueue(qName)
    }
    assertNotEquals(q.name, qName)

    ac.createQueue(qName)
    q = ac.getQueue(qName)
    assertEquals(q.name, qName)

    ac.deleteQueue(qName)
    q = new Queue()
    intercept[StatusRuntimeException] {
      q = ac.getQueue(qName)
    }
    assertNotEquals(q.name, qName)
  }
}
