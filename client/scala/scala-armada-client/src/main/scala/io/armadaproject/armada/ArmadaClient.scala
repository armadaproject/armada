package io.armadaproject.armada

import api.job.{JobStatusRequest, JobStatusResponse, JobsGrpc}
import api.event.EventGrpc
import api.submit.{SubmitGrpc, JobSubmitRequest, JobSubmitResponse, JobSubmitRequestItem}
import api.health.HealthCheckResponse
import api.submit.Job
import k8s.io.api.core.v1.generated.{Container, PodSpec, ResourceRequirements}
import k8s.io.apimachinery.pkg.api.resource.generated.Quantity
import com.google.protobuf.empty.Empty
import io.grpc.{ManagedChannelBuilder, ManagedChannel}

class ArmadaClient(var channel: ManagedChannel) {
  def SubmitJobs(queue: String, jobSetId: String, jobRequestItems: Seq[JobSubmitRequestItem]): JobSubmitResponse = {
    val blockingStub = SubmitGrpc.blockingStub(channel)
    blockingStub.submitJobs(JobSubmitRequest(queue, jobSetId, jobRequestItems))
  }

  def GetJobStatus(jobId: String): JobStatusResponse = {
    val blockingStub = JobsGrpc.blockingStub(channel)
    blockingStub.getJobStatus(JobStatusRequest(jobIds = Seq(jobId)))
  }

  def EventHealth(): HealthCheckResponse.ServingStatus = {
    val blockingStub = EventGrpc.blockingStub(channel)
    blockingStub.health(Empty()).status
  }

  def SubmitHealth(): HealthCheckResponse.ServingStatus = {
    val blockingStub = SubmitGrpc.blockingStub(channel)
    blockingStub.health(Empty()).status
  }

}

object ArmadaClient {
  // TODO: SSL
  def GetChannel(host: String, port: Int): ManagedChannel = {
    ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
  }
}
