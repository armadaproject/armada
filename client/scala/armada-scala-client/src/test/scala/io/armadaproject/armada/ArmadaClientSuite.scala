package io.armadaproject.armada

import api.submit.{
  BatchQueueCreateResponse,
  BatchQueueUpdateResponse,
  CancellationResult,
  Job,
  JobCancelRequest,
  JobPreemptRequest,
  JobReprioritizeRequest,
  JobReprioritizeResponse,
  JobSetCancelRequest,
  JobState,
  JobSubmitRequest,
  JobSubmitRequestItem,
  JobSubmitResponse,
  JobSubmitResponseItem,
  PreemptionResult,
  Queue,
  QueueDeleteRequest,
  QueueGetRequest,
  QueueList,
  StreamingQueueGetRequest,
  StreamingQueueMessage,
  SubmitGrpc
}
import api.job.{
  GetActiveQueuesRequest,
  GetActiveQueuesResponse,
  JobDetailsRequest,
  JobDetailsResponse,
  JobErrorsRequest,
  JobErrorsResponse,
  JobRunDetailsRequest,
  JobRunDetailsResponse,
  JobStatusRequest,
  JobStatusResponse,
  JobStatusUsingExternalJobUriRequest,
  JobsGrpc
}
import com.google.protobuf.empty.Empty
import api.health.HealthCheckResponse
import api.event.{
  EventGrpc,
  EventMessage,
  EventStreamMessage,
  JobPendingEvent,
  JobQueuedEvent,
  JobRunningEvent,
  JobSetRequest,
  JobSucceededEvent,
  WatchRequest
}
import api.event.EventMessage.Events

import io.grpc.{Server, ServerBuilder, Status, StatusRuntimeException}
import jkugiya.ulid.ULID
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random

private class EventMockServer(
    jobMap: TrieMap[String, Job],
    queueMap: TrieMap[String, Queue],
    statusMap: TrieMap[String, JobState]
) extends EventGrpc.Event {

  val ulidGen = ULID.getGenerator()

  override def health(
      empty: Empty
  ): scala.concurrent.Future[HealthCheckResponse] = {
    Future.successful(
      HealthCheckResponse(HealthCheckResponse.ServingStatus.SERVING)
    )
  }

  override def getJobSetEvents(
      request: JobSetRequest,
      responseObserver: io.grpc.stub.StreamObserver[EventStreamMessage]
  ): Unit = {
    // TODO: fill-in
  }

  override def watch(
      request: WatchRequest,
      responseObserver: io.grpc.stub.StreamObserver[EventStreamMessage]
  ): Unit = {
    // Verify queue exists
    if (!queueMap.contains(request.queue)) {
      responseObserver.onError(
        new StatusRuntimeException(
          Status.NOT_FOUND.withDescription(
            s"Queue '${request.queue}' not found"
          )
        )
      )
      return
    }

    // Find all jobs in the specified jobSet
    val matchingJobs = jobMap.filter { case (_, job) =>
      job.jobSetId == request.jobSetId
    }

    // Emit events for each job showing the usual (not all possible) transition states
    matchingJobs.foreach { case (jobId, job) =>
      if (statusMap.contains(jobId)) {
        // This is a purposely fixed and basic flow of events, emitting a subset of the
        // typical state-change events that a successful Armada job would experience.
        // We do not emit new events for a job based upon the current state of a job; the
        // goal is to verify that the Armada client's jobWatch() method correctly
        // iterates through all the received event messages and returns them to its
        // caller in the correct order (i.e. as we emit them here). Higher-order testing of
        // job-state event transitions is done in the Armada core code (outside each
        // client library implementation).

        // QUEUED
        val queuedEvent = EventStreamMessage(
          id = ulidGen.base32().toLowerCase(),
          message = Option[EventMessage](
            EventMessage(
              api.event.EventMessage.Events.Queued(
                new JobQueuedEvent(jobId, job.jobSetId, job.queue)
              )
            )
          )
        )
        statusMap.put(jobId, JobState.QUEUED)
        responseObserver.onNext(queuedEvent)

        // PENDING
        val pendingEvent = EventStreamMessage(
          id = ulidGen.base32().toLowerCase(),
          message = Option[EventMessage](
            EventMessage(
              api.event.EventMessage.Events.Pending(
                new JobPendingEvent(jobId, job.jobSetId, job.queue)
              )
            )
          )
        )
        statusMap.put(jobId, JobState.PENDING)
        responseObserver.onNext(pendingEvent)

        // RUNNING
        val runningEvent = EventStreamMessage(
          id = ulidGen.base32().toLowerCase(),
          message = Option[EventMessage](
            EventMessage(
              api.event.EventMessage.Events.Running(
                new JobRunningEvent(jobId, job.jobSetId, job.queue)
              )
            )
          )
        )
        statusMap.put(jobId, JobState.RUNNING)
        responseObserver.onNext(runningEvent)

        // SUCCEEDED
        val succeededEvent = EventStreamMessage(
          id = ulidGen.base32().toLowerCase(),
          message = Option[EventMessage](
            EventMessage(
              api.event.EventMessage.Events.Succeeded(
                new JobSucceededEvent(jobId, job.jobSetId, job.queue)
              )
            )
          )
        )
        statusMap.put(jobId, JobState.SUCCEEDED)
        responseObserver.onNext(succeededEvent)
      } else {
        // Job exists in jobMap but not in statusMap - this shouldn't happen in normal flow
        responseObserver.onError(
          new StatusRuntimeException(
            Status.INTERNAL.withDescription(s"Job '$jobId' has no status")
          )
        )
      }
    }

    responseObserver.onCompleted()
  }
}

private class SubmitMockServer(
    jobMap: TrieMap[String, Job], // jobId -> Job
    queueMap: TrieMap[String, Queue], // queueName -> Queue
    statusMap: TrieMap[String, JobState] // jobId -> JobState
) extends SubmitGrpc.Submit {

  val ulidGen = ULID.getGenerator()

  def cancelJobSet(cancelReq: JobSetCancelRequest): Future[Empty] = {
    jobMap.foreach {
      case (jobId, job) => {
        if (job.jobSetId == cancelReq.jobSetId) {
          statusMap.put(jobId, JobState.CANCELLED)
        }
      }
    }

    Future.successful(new Empty)
  }

  def cancelJobs(
      cancelReq: JobCancelRequest
  ): scala.concurrent.Future[CancellationResult] = {
    val res = new CancellationResult()

    cancelReq.jobIds.foreach { jobId =>
      if (jobMap.contains(jobId) && statusMap.contains(jobId)) {
        statusMap.put(jobId, JobState.CANCELLED)
        res.addCancelledIds(jobId)
      }
    }

    Future.successful(res)
  }

  def createQueue(
      request: Queue
  ): scala.concurrent.Future[com.google.protobuf.empty.Empty] = {
    queueMap.put(request.name, request)
    Future.successful(new Empty)
  }

  def createQueues(
      request: QueueList
  ): scala.concurrent.Future[BatchQueueCreateResponse] = {
    request.queues.foreach { q => queueMap.put(q.name, q) }
    Future.successful(new BatchQueueCreateResponse)
  }

  def deleteQueue(
      request: QueueDeleteRequest
  ): scala.concurrent.Future[com.google.protobuf.empty.Empty] = {
    queueMap.remove(request.name)
    Future.successful(new Empty)
  }

  def getQueue(request: QueueGetRequest): scala.concurrent.Future[Queue] = {
    val result: Option[Queue] = queueMap.get(request.name)
    result match {
      case Some(queueFound) => Future.successful(queueFound)
      case None => Future.failed(new StatusRuntimeException(Status.NOT_FOUND))
    }
  }

  def getQueues(
      request: StreamingQueueGetRequest,
      responseObserver: io.grpc.stub.StreamObserver[StreamingQueueMessage]
  ): Unit = {
    Future.successful(new StreamingQueueMessage)
  }

  def health(
      request: com.google.protobuf.empty.Empty
  ): scala.concurrent.Future[HealthCheckResponse] = {
    Future.successful(
      HealthCheckResponse(HealthCheckResponse.ServingStatus.SERVING)
    )
  }

  def preemptJobs(
      request: JobPreemptRequest
  ): scala.concurrent.Future[PreemptionResult] = {
    Future.successful(new PreemptionResult)
  }

  def reprioritizeJobs(
      request: JobReprioritizeRequest
  ): scala.concurrent.Future[JobReprioritizeResponse] = {
    Future.successful(new JobReprioritizeResponse)
  }

  def submitJobs(
      request: JobSubmitRequest
  ): scala.concurrent.Future[JobSubmitResponse] = {
    val result: Option[Queue] = queueMap.get(request.queue)
    result match {
      case Some(_) => {
        val jobId: String = ulidGen.base32().toLowerCase()
        val newJob = (new Job()).withJobSetId(request.jobSetId)
        jobMap.put(jobId, newJob)
        statusMap.put(jobId, JobState.RUNNING)

        Future.successful(
          new JobSubmitResponse(List(JobSubmitResponseItem(jobId)))
        )
      }
      case None => {
        val msg = "could not find queue \"" + request.queue + "\""
        Future.failed(
          new StatusRuntimeException(
            Status.PERMISSION_DENIED.withDescription(msg)
          )
        )
      }
    }

  }

  def updateQueue(
      request: Queue
  ): scala.concurrent.Future[com.google.protobuf.empty.Empty] = {
    Future.successful(new Empty)
  }

  def updateQueues(
      request: QueueList
  ): scala.concurrent.Future[BatchQueueUpdateResponse] = {
    Future.successful(new BatchQueueUpdateResponse)
  }
}

private class JobsMockServer(
    statusMap: TrieMap[String, JobState]
) extends JobsGrpc.Jobs {

  def getActiveQueues(
      request: GetActiveQueuesRequest
  ): scala.concurrent.Future[GetActiveQueuesResponse] = {
    Future.successful(new GetActiveQueuesResponse)
  }

  def getJobDetails(
      request: JobDetailsRequest
  ): scala.concurrent.Future[JobDetailsResponse] = {
    Future.successful(new JobDetailsResponse)
  }

  def getJobErrors(
      request: JobErrorsRequest
  ): scala.concurrent.Future[JobErrorsResponse] = {
    Future.successful(new JobErrorsResponse)
  }

  def getJobRunDetails(
      request: JobRunDetailsRequest
  ): scala.concurrent.Future[JobRunDetailsResponse] = {
    Future.successful(new JobRunDetailsResponse)
  }

  def getJobStatus(
      request: JobStatusRequest
  ): scala.concurrent.Future[JobStatusResponse] = {
    val statuses = collection.mutable.Map[String, JobState]()

    for (jobId <- request.jobIds) {
      val result: Option[JobState] = statusMap.get(jobId)
      result match {
        case Some(jobState) => statuses.put(jobId, jobState)
        case None => Future.failed(new StatusRuntimeException(Status.NOT_FOUND))
      }
    }

    Future.successful(
      new JobStatusResponse(statuses.toMap)
    )
  }

  def getJobStatusUsingExternalJobUri(
      request: JobStatusUsingExternalJobUriRequest
  ): scala.concurrent.Future[JobStatusResponse] = {
    Future.successful(new JobStatusResponse)
  }
}

class ArmadaClientSuite
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {
  val testPort = 12345

  private var server: Server = null
  private val jobMap: TrieMap[String, Job] = new TrieMap // key is job id
  private val queueMap: TrieMap[String, Queue] =
    new TrieMap // key is queue name
  private val statusMap: TrieMap[String, JobState] =
    new TrieMap // key is job id

  // Required for processing Futures in test
  implicit val ec: scala.concurrent.ExecutionContext =
    scala.concurrent.ExecutionContext.global

  override def beforeAll(): Unit = {
    import scala.concurrent.ExecutionContext
    server = ServerBuilder
      .forPort(testPort)
      .addService(
        EventGrpc.bindService(
          new EventMockServer(jobMap, queueMap, statusMap),
          ExecutionContext.global
        )
      )
      .addService(
        SubmitGrpc.bindService(
          new SubmitMockServer(jobMap, queueMap, statusMap),
          ExecutionContext.global
        )
      )
      .addService(
        JobsGrpc
          .bindService(new JobsMockServer(statusMap), ExecutionContext.global)
      )
      .build()
      .start()
  }

  override def afterAll(): Unit = {
    server.shutdown()
  }

  override def beforeEach(): Unit = {
    jobMap.clear()
    queueMap.clear()
    statusMap.clear()
  }

  test("ArmadaClient.eventHealth()") {
    val ac = ArmadaClient("localhost", testPort)
    val status = ac.eventHealth()
    assert(status === HealthCheckResponse.ServingStatus.SERVING)
  }

  test("ArmadaClient.submitHealth()") {
    val ac = ArmadaClient("localhost", testPort)
    val healthResp = ac.submitHealth()
    val resp = Await.result(healthResp, 10.seconds)
    assert(resp.status === HealthCheckResponse.ServingStatus.SERVING)
  }

  test("ArmadaClient.submitJobs()") {
    val ac = ArmadaClient("localhost", testPort)

    // submission to non-existent queue
    val qName = "nonexistent-queue-" + Random.alphanumeric.take(8).mkString
    var response: JobSubmitResponse = new JobSubmitResponse()

    intercept[StatusRuntimeException] {
      response =
        ac.submitJobs(qName, "testJobSetId", List(new JobSubmitRequestItem()))
    }
    assert(response.jobResponseItems.length === 0)

    // submission to existing queue
    ac.createQueue(qName)
    response =
      ac.submitJobs(qName, "testJobSetId", List(new JobSubmitRequestItem()))
    assert(response.jobResponseItems.length === 1)
    ac.deleteQueue(qName)
  }

  test("ArmadaClient.getJobStatus()") {
    val ac = ArmadaClient("localhost", testPort)
    val qName =
      "getjobstatus-test-queue-" + Random.alphanumeric.take(8).mkString

    ac.createQueue(qName)
    val newJob =
      ac.submitJobs(qName, "testJobSetId", List(new JobSubmitRequestItem()))

    val jobId = newJob.jobResponseItems(0).jobId
    val jobStatus = ac.getJobStatus(jobId)
    assert(jobStatus.jobStates(jobId).isRunning)

    ac.deleteQueue(qName)
  }

  test("ArmadaClient.cancelJobs()") {
    val ac = ArmadaClient("localhost", testPort)
    val qName = "queue-" + Random.alphanumeric.take(8).mkString
    var jobs = Seq[String]()

    ac.createQueue(qName)

    // Submit 3 jobs
    for (_ <- 1 to 3) {
      val response =
        ac.submitJobs(qName, "testJobSetId", List(new JobSubmitRequestItem()))
      assert(response.jobResponseItems.length === 1)
      jobs = jobs :+ response.jobResponseItems(0).jobId
    }

    // Cancel the first two of the jobs and validate they are cancelled
    val cancelRes =
      ac.cancelJobs(new JobCancelRequest().withJobIds(jobs.take(2)))

    Await.result(cancelRes, 10.seconds)

    // Verify both cancelled jobs
    for (i <- 0 to 1) {
      val jobStatus = ac.getJobStatus(jobs(i))
      assert(jobStatus.jobStates(jobs(i)) === JobState.CANCELLED)
    }

    // The third job should still be running
    assert(ac.getJobStatus(jobs(2)).jobStates(jobs(2)).isRunning)

    ac.deleteQueue(qName)
  }

  test("ArmadaClient.cancelJobSet()") {
    val ac = ArmadaClient("localhost", testPort)
    val qName = "queue-" + Random.alphanumeric.take(8).mkString
    val cancelJobSetId = "jobset-666"
    var cancelJobIds = Seq[String]()

    ac.createQueue(qName)

    // Submit 2 jobs in a job set that will be cancelled
    for (_ <- 1 to 2) {
      val response =
        ac.submitJobs(qName, cancelJobSetId, List(new JobSubmitRequestItem()))
      assert(response.jobResponseItems.length === 1)
      cancelJobIds = cancelJobIds :+ response.jobResponseItems(0).jobId
    }

    // Submit one job that will be allowed to run
    val response =
      ac.submitJobs(qName, "runJobSet", List(new JobSubmitRequestItem()))
    assert(response.jobResponseItems.length === 1)
    val runJobId = response.jobResponseItems(0).jobId

    ac.cancelJobSet(cancelJobSetId)

    for (cancelJob <- cancelJobIds) {
      val jobStatus = ac.getJobStatus(cancelJob)
      assert(jobStatus.jobStates(cancelJob) === JobState.CANCELLED)
    }

    // The job not in that job set should still be running
    assert(ac.getJobStatus(runJobId).jobStates(runJobId).isRunning)

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
    assert(q.name !== qName)

    ac.createQueue(qName)
    q = ac.getQueue(qName)
    assert(q.name === qName)

    ac.deleteQueue(qName)
    q = new Queue()
    intercept[StatusRuntimeException] {
      q = ac.getQueue(qName)
    }
    assert(q.name !== qName)
  }

  test("ArmadaClient.jobWatcher()") {
    val ac = ArmadaClient("localhost", testPort)
    val qName = "queue-" + Random.alphanumeric.take(8).mkString
    val watchJobSetId = "jobset-666"

    ac.createQueue(qName)

    val response =
      ac.submitJobs(qName, watchJobSetId, List(new JobSubmitRequestItem()))
    assert(response.jobResponseItems.length === 1)

    var watchJobIds = Seq[String](response.jobResponseItems(0).jobId)

    // Required for processing Futures in test
    implicit val ec: scala.concurrent.ExecutionContext =
      scala.concurrent.ExecutionContext.global

    // call jobWatcher and observe if it gets all the expected events
    val watchIter =
      ac.jobWatcher(q = qName, jobSet = watchJobSetId, lastMessage = "")

    var evs = Seq[String]()
    while (watchIter.hasNext) {
      var evStreamMsg = watchIter.next()

      evStreamMsg.message match {
        case Some(msg) =>
          evs = evs :+ getEventStatus(msg)
        case None =>
          fail(
            "jobWatcher() test failed: test event stream entry had no EventMessage "
          )
      }
    }

    val expectedStates =
      Seq[String]("QUEUED", "PENDING", "RUNNING", "SUCCEEDED")

    assert(expectedStates.length == evs.length)
    // Check that events arrived in the expected order
    for (n <- 0 to (expectedStates.length - 1)) {
      assert(expectedStates(n) == evs(n))
    }

    ac.deleteQueue(qName)
  }

  // Given an EventMessage object, derive the basic Armada event
  // status, e.g. "PENDING", "RUNNING", "SUCCEEDED", etc.
  def getEventStatus(eventMessage: EventMessage): String =
    eventMessage.events match {
      case Events.Empty => "UNKNOWN"
      case e =>
        e.productPrefix.replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase
    }
}
