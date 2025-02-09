package io.armadaproject.armada

import k8s.io.api.core.v1.generated.{Container, PodSpec, ResourceRequirements}
import k8s.io.apimachinery.pkg.api.resource.generated.Quantity

object Main {
  val host = "localhost"
  val port = 30002

  def main(args: Array[String]): Unit = {
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

    val ac = ArmadaClient("localhost", 30002)
    val response = ac.submitJobs("testQueue", "testJobSetId", List(testJob))

    println(s"Job Submit Response")
    for (respItem <- response.jobResponseItems) {
      println(s"JobID: ${respItem.jobId}  Error: ${respItem.error} ")
    }
  }
}
