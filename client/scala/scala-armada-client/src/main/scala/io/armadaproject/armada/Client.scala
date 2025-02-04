package io.armadaproject.armada

import api.event.EventGrpc
import api.health.HealthCheckResponse
import api.submit.SubmitGrpc
import api.job.{JobStatusRequest, JobStatusResponse, JobsGrpc}
import api.submit.{JobSubmitResponse, Queue, QueueDeleteRequest, QueueGetRequest, SubmitGrpc}
import com.google.protobuf.empty.Empty
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.time.Duration
import k8s.io.api.core.v1.generated.{Container, PodSpec, ResourceRequirements}
import k8s.io.apimachinery.pkg.api.resource.generated.Quantity

class Client(var chan: ManagedChannel, var eventTimed: Duration) {
  def this(host: String, port: Int) = {
    this(
      ManagedChannelBuilder.forAddress(host, port).usePlaintext().build,
      Duration.ofSeconds(60)
    )
  }

  def getHealth(): HealthCheckResponse.ServingStatus = {
    val blockingStub = EventGrpc.blockingStub(chan)
    val reply: HealthCheckResponse = blockingStub.health(Empty())
    return reply.status
  }

  def createQueue(name: String): Unit = {
    val blockingStub = SubmitGrpc.blockingStub(chan)
    val q = api.submit.Queue().withName(name).withPriorityFactor(1)
    blockingStub.createQueue(q)
  }

  def deleteQueue(name: String): Unit = {
    val qReq = QueueDeleteRequest(name)
    val blockingStub = SubmitGrpc.blockingStub(chan)
    blockingStub.deleteQueue(qReq)
  }

  def getQueue(name: String): Queue = {
    val qReq = QueueGetRequest(name)
    val blockingStub = SubmitGrpc.blockingStub(chan)
    blockingStub.getQueue(qReq)
  }

  def getJobStatus(host: String, port: Int, jId: String): JobStatusResponse = {
    val blockingStub = JobsGrpc.blockingStub(chan)
    val jsReq = JobStatusRequest(jobIds = Seq(jId))
    val reply: JobStatusResponse = blockingStub.getJobStatus(jsReq)
    reply
  }

  def submitJob(): JobSubmitResponse = {
    val sleepContainer = Container()
      .withName("ls")
      .withImagePullPolicy("IfNotPresent")
      .withImage("alpine:3.10")
      .withCommand(Seq("ls"))
      .withArgs(
        Seq(
          "-c",
          "ls -l; sleep 30; date; echo '========'; ls -l; sleep 10; date"
        )
      )
      .withResources(
        ResourceRequirements(
          limits = Map(
            "memory" -> Quantity(Option("10Mi")),
            "cpu" -> Quantity(Option("100m"))
          ),
          requests = Map(
            "memory" -> Quantity(Option("10Mi")),
            "cpu" -> Quantity(Option("100m"))
          )
        )
      )

    val podSpec = PodSpec()
      .withTerminationGracePeriodSeconds(0)
      .withRestartPolicy("Never")
      .withContainers(Seq(sleepContainer))

    val testJob = api.submit
      .JobSubmitRequestItem()
      .withPriority(0)
      .withNamespace("personal-anonymous")
      .withPodSpec(podSpec)

    val jobReq = api.submit.JobSubmitRequest(
      queue = "e2e-test-queue",
      jobSetId = "spark-test-1",
      jobRequestItems = Seq(testJob)
    )

    val blockingStub = SubmitGrpc.blockingStub(chan)
    val jobSubmitResponse = blockingStub.submitJobs(jobReq)

    jobSubmitResponse
  }
}
