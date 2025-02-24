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

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.{Failure, Random, Success}
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

private class SubmitMockServer(
  jobMap: TrieMap[String, Job],
  queueMap: TrieMap[String, Queue],
  statusMap: TrieMap[String, JobState]
) extends SubmitGrpc.Submit {

  val ulidGen = ULID.getGenerator()

  def cancelJobSet(request: JobSetCancelRequest): scala.concurrent.Future[com.google.protobuf.empty.Empty] = {
    jobMap.foreach {
      case (jobId, job) => if (job.jobSetId == request.jobSetId) { statusMap.put(jobId, JobState.CANCELLED) }
    }

    Future.successful(new Empty)
  }

  def cancelJobs(cancelReq: JobCancelRequest): scala.concurrent.Future[CancellationResult] = {
    val res = new CancellationResult()

    cancelReq.jobIds.foreach { jobId =>
      if (jobMap.contains(jobId) && statusMap.contains(jobId)) {
        statusMap.put(jobId, JobState.CANCELLED)
        res.addCancelledIds(jobId)
      }
    }

    Future.successful(res)
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
    val result: Option[Queue] = queueMap.get(request.name)
    result match {
      case Some(queueFound) => Future.successful(queueFound)
      case None =>  Future.failed(new StatusRuntimeException(Status.NOT_FOUND))
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
    val result: Option[Queue] = queueMap.get(request.queue)
    result match {
      case Some(queueFound) => {
        val jobId: String = ulidGen.base32().toLowerCase()
        val newJob = new Job()
        jobMap.put(jobId, newJob)
        statusMap.put(jobId, JobState.RUNNING)

        Future.successful(new JobSubmitResponse(List(JobSubmitResponseItem(jobId))))
      }
      case None => {
        val msg = "could not find queue \"" + request.queue + "\""
        Future.failed(new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription(msg)))
      }
    }
  }

  def updateQueue(request: Queue): scala.concurrent.Future[com.google.protobuf.empty.Empty] = {
    Future.successful(new Empty)
  }

  def updateQueues(request: QueueList): scala.concurrent.Future[BatchQueueUpdateResponse] = {
    Future.successful(new BatchQueueUpdateResponse)
  }
}

private class JobsMockServer(
  jobMap: TrieMap[String, Job],
  statusMap: TrieMap[String, JobState]
) extends JobsGrpc.Jobs {

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
    val statuses = collection.mutable.Map[String,JobState]()
    for (jobId <- request.jobIds) {
      val result: Option[JobState] = statusMap.get(jobId)
      result match {
        case Some(jobState) =>  statuses.put(jobId, jobState)
        case None => Future.failed(new StatusRuntimeException(Status.NOT_FOUND))
      }
    }

    Future.successful(new JobStatusResponse(statuses.to(collection.immutable.Map)))
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

    private var jobMap: TrieMap[String, Job] = new TrieMap       // key is job id
    private var queueMap: TrieMap[String, Queue] = new TrieMap   // key is queue name
    private var statusMap: TrieMap[String, JobState] = new TrieMap  // key is job id

    override def beforeAll(): Unit = {
      import scala.concurrent.ExecutionContext
      server = ServerBuilder
        .forPort(testPort)
        .addService(EventGrpc.bindService(new EventMockServer, ExecutionContext.global))
        .addService(SubmitGrpc.bindService(new SubmitMockServer(jobMap, queueMap, statusMap), ExecutionContext.global))
        .addService(JobsGrpc.bindService(new JobsMockServer(jobMap, statusMap), ExecutionContext.global))
        .build()
        .start()
    }
    override def afterAll(): Unit = {
      server.shutdown()
    }
  }

  override def munitFixtures = List(mockEventServer)

  test("ArmadaClient.eventHealth()") {
    val ac = ArmadaClient("localhost", testPort)
    val status = ac.eventHealth()
    assertEquals(status, HealthCheckResponse.ServingStatus.SERVING)
  }

  test("ArmadaClient.submitHealth()") {
    val ac = ArmadaClient("localhost", testPort)
    val status = ac.submitHealth()
    assertEquals(status, HealthCheckResponse.ServingStatus.SERVING)
  }

  test("ArmadaClient.submitJobs()") {
    val ac = ArmadaClient("localhost", testPort)

    // submission to non-existent queue
    val qName = "nonexistent-queue-" + Random.alphanumeric.take(8).mkString
    var response: JobSubmitResponse = new JobSubmitResponse()

    intercept[StatusRuntimeException] {
      response = ac.submitJobs(qName, "testJobSetId", List(new JobSubmitRequestItem()))
    }
    assertEquals(0, response.jobResponseItems.length)

    // submission to existing queue
    ac.createQueue(qName)
    response = ac.submitJobs(qName, "testJobSetId", List(new JobSubmitRequestItem()))
    assertEquals(1, response.jobResponseItems.length)
    ac.deleteQueue(qName)
  }

  test("ArmadaClient.getJobStatus()") {
    val ac = ArmadaClient("localhost", testPort)
    val qName = "getjobstatus-test-queue-" + Random.alphanumeric.take(8).mkString

    ac.createQueue(qName)
    val newJob = ac.submitJobs(qName, "testJobSetId", List(new JobSubmitRequestItem()))

    val jobId = newJob.jobResponseItems(0).jobId
    val jobStatus = ac.getJobStatus(jobId)
    assert(jobStatus.jobStates(jobId).isRunning)

    ac.deleteQueue(qName)
  }

  test("ArmadaClient.cancelJobs()") {
    val ac = ArmadaClient("localhost", testPort)
    val qName = "nonexistent-queue-" + Random.alphanumeric.take(8).mkString
    var jobs = Seq[String]()

    ac.createQueue(qName)

    // Submit 3 jobs
    for (i <- 1 to 3) {
      val response = ac.submitJobs(qName, "testJobSetId", List(new JobSubmitRequestItem()))
      assertEquals(1, response.jobResponseItems.length)
      jobs = jobs :+ response.jobResponseItems(0).jobId
    }

    // Required for processing Futures in test
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

    // Cancel the first two of the jobs and validate they are cancelled
    val cancelRes = ac.cancelJobs(new JobCancelRequest().withJobIds(Seq[String](jobs(0), jobs(1))))

    cancelRes.onComplete {
      case Success(cancelResult) => {
        for (i <- 0 to 1) {
          val jobStatus = ac.getJobStatus(jobs(i))
          assertEquals(true, jobStatus.jobStates(jobs(i)).isCancelled)
        }
      }
      case Failure(cancelResult) => fail("cancelJobs() test failed")
    }

    // The third job should still be running
    assert(ac.getJobStatus(jobs(2)).jobStates(jobs(2)).isRunning)

    ac.deleteQueue(qName)
  }

  test("ArmadaClient.{get,create,delete}Queue()") {
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
