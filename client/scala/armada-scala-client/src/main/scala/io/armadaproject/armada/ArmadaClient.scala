package io.armadaproject.armada

import api.job.{JobStatusRequest, JobStatusResponse, JobsGrpc}
import api.event.EventGrpc
import api.submit.{
  CancellationResult,
  JobCancelRequest,
  JobSetCancelRequest,
  JobSubmitRequest,
  JobSubmitRequestItem,
  JobSubmitResponse,
  Queue,
  QueueDeleteRequest,
  QueueGetRequest,
  SubmitGrpc
}
import api.health.HealthCheckResponse
import com.google.protobuf.empty.Empty
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import scala.concurrent.Future

class ArmadaClient(channel: ManagedChannel) {
  def submitJobs(
      queue: String,
      jobSetId: String,
      jobRequestItems: Seq[JobSubmitRequestItem]
  ): JobSubmitResponse = {
    val blockingStub = SubmitGrpc.blockingStub(channel)
    blockingStub.submitJobs(JobSubmitRequest(queue, jobSetId, jobRequestItems))
  }

  def getJobStatus(jobId: String): JobStatusResponse = {
    val blockingStub = JobsGrpc.blockingStub(channel)
    blockingStub.getJobStatus(JobStatusRequest(jobIds = Seq(jobId)))
  }

  def cancelJobs(
      cancelReq: JobCancelRequest
  ): scala.concurrent.Future[CancellationResult] = {
    SubmitGrpc.stub(channel).cancelJobs(cancelReq)
  }

  def cancelJobSet(jobSetId: String): Future[Empty] = {
    SubmitGrpc.blockingStub(channel).cancelJobSet(JobSetCancelRequest(jobSetId))
    Future.successful(new Empty)
  }

  def eventHealth(): HealthCheckResponse.ServingStatus = {
    val blockingStub = EventGrpc.blockingStub(channel)
    blockingStub.health(Empty()).status
  }

  def submitHealth(): HealthCheckResponse.ServingStatus = {
    val blockingStub = SubmitGrpc.blockingStub(channel)
    blockingStub.health(Empty()).status
  }

  def createQueue(name: String): Unit = {
    val blockingStub = SubmitGrpc.blockingStub(channel)
    val q = api.submit.Queue().withName(name).withPriorityFactor(1)
    blockingStub.createQueue(q)
  }

  def deleteQueue(name: String): Unit = {
    val qReq = QueueDeleteRequest(name)
    val blockingStub = SubmitGrpc.blockingStub(channel)
    blockingStub.deleteQueue(qReq)
  }

  def getQueue(name: String): Queue = {
    val qReq = QueueGetRequest(name)
    val blockingStub = SubmitGrpc.blockingStub(channel)
    blockingStub.getQueue(qReq)
  }
}

object ArmadaClient {
  // TODO: SSL
  def apply(channel: ManagedChannel): ArmadaClient = {
    new ArmadaClient(channel)
  }

  def apply(host: String, port: Int): ArmadaClient = {
    val channel =
      ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    ArmadaClient(channel)
  }
}
