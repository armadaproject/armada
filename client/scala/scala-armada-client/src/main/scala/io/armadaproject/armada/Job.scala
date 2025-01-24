package io.armadaproject.armada

import io.grpc.ManagedChannelBuilder
import k8s.io.api.core.v1.generated.{Container, PodSpec, ResourceRequirements}
import k8s.io.apimachinery.pkg.api.resource.generated.Quantity
import api.submit.SubmitGrpc
import api.job.{JobStatusRequest, JobStatusResponse, JobsGrpc}

object Job {
  val host = "localhost"
  val port = 30002

  def getJobStatus(host: String, port: Int, jId: String): JobStatusResponse = {
    val channel =
      ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
    val blockingStub = JobsGrpc.blockingStub(channel)

    val jsReq = JobStatusRequest(jobIds = Seq(jId))
    val reply: JobStatusResponse = blockingStub.getJobStatus(jsReq)
    reply
  }

  def submitJob(): Unit = {
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

    val testJobRequest = api.submit.JobSubmitRequest(
      queue = "e2e-test-queue",
      jobSetId = "spark-test-1",
      jobRequestItems = Seq(testJob)
    )

    val channel =
      ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
    val blockingStub = SubmitGrpc.blockingStub(channel)

    val jobSubmitResponse = blockingStub.submitJobs(testJobRequest)

    println(s"Job Submit Response")
    for (respItem <- jobSubmitResponse.jobResponseItems) {
      println(s"JobID: ${respItem.jobId}  Error: ${respItem.error} ")
    }
  }
}
