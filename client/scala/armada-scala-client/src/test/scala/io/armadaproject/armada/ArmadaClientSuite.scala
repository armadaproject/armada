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
  Queue,
  QueueDeleteRequest,
  QueueGetRequest,
  QueueList,
  StreamingQueueGetRequest,
  StreamingQueueMessage,
  SubmitGrpc
}
import api.job.{
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
import api.event.{EventGrpc, EventStreamMessage, JobSetRequest, WatchRequest}
import io.grpc.{Server, ServerBuilder, Status, StatusRuntimeException}
import jkugiya.ulid.ULID
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.util.{Failure, Random, Success}

private class EventMockServer extends EventGrpc.Event {
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
    // TODO: fill-in
  }
}

private class SubmitMockServer(
    jobMap: TrieMap[String, Job],
    queueMap: TrieMap[String, Queue],
    statusMap: TrieMap[String, JobState]
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
  ): scala.concurrent.Future[com.google.protobuf.empty.Empty] = {
    Future.successful(new Empty)
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

class ArmadaClientSuite extends AnyFunSuite with BeforeAndAfterAll {
  val testPort = 12345

  private var server: Server = null
  private val jobMap: TrieMap[String, Job] = new TrieMap // key is job id
  private val queueMap: TrieMap[String, Queue] =
    new TrieMap // key is queue name
  private val statusMap: TrieMap[String, JobState] =
    new TrieMap // key is job id

  override def beforeAll(): Unit = {
    import scala.concurrent.ExecutionContext
    server = ServerBuilder
      .forPort(testPort)
      .addService(
        EventGrpc.bindService(new EventMockServer, ExecutionContext.global)
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

  test("ArmadaClient.eventHealth()") {
    val ac = ArmadaClient("localhost", testPort)
    val status = ac.eventHealth()
    assert(status === HealthCheckResponse.ServingStatus.SERVING)
  }

  test("ArmadaClient.submitHealth()") {
    val ac = ArmadaClient("localhost", testPort)
    val status = ac.submitHealth()
    assert(status === HealthCheckResponse.ServingStatus.SERVING)
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

    // Required for processing Futures in test
    implicit val ec: scala.concurrent.ExecutionContext =
      scala.concurrent.ExecutionContext.global

    // Cancel the first two of the jobs and validate they are cancelled
    val cancelRes =
      ac.cancelJobs(new JobCancelRequest().withJobIds(jobs.take(2)))

    cancelRes.onComplete {
      case Success(_) => {
        // Verify both cancelled jobs
        for (i <- 0 to 1) {
          val jobStatus = ac.getJobStatus(jobs(i))
          assert(jobStatus.jobStates(jobs(i)) === JobState.CANCELLED)
        }

        // The third job should still be running
        assert(ac.getJobStatus(jobs(2)).jobStates(jobs(2)).isRunning)
      }
      case Failure(_) => fail("cancelJobs() test failed")
    }

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

    // Required for processing Futures in test
    implicit val ec: scala.concurrent.ExecutionContext =
      scala.concurrent.ExecutionContext.global

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
}
