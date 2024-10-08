package io.armadaproject;

import api.SubmitOuterClass.JobSubmitRequest;
import api.SubmitOuterClass.JobSubmitRequestItem;
import api.SubmitOuterClass.JobSubmitResponse;
import java.util.List;
import java.util.logging.Logger;
import k8s.io.api.core.v1.Generated.Container;
import k8s.io.api.core.v1.Generated.PodSpec;
import k8s.io.api.core.v1.Generated.ResourceRequirements;
import k8s.io.apimachinery.pkg.api.resource.Generated.Quantity;

public class SubmitExample {

  private static final Logger LOG = Logger.getLogger(SubmitExample.class.getName());

  public static void main(String[] args) {
    ArmadaClient armadaClient = new ArmadaClient("localhost", 30002);

    String queueName = "example";

    JobSubmitRequest jobSubmitRequest = JobSubmitRequest.newBuilder()
        .setQueue(queueName)
        .setJobSetId(queueName)
        .addJobRequestItems(JobSubmitRequestItem.newBuilder()
            .setNamespace("default")
            .setPriority(0.00d)
            .addPodSpecs(PodSpec.newBuilder()
                .setPriorityClassName("armada-default")
                .setTerminationGracePeriodSeconds(0L)
                .setRestartPolicy("Never")
                .addContainers(Container.newBuilder()
                    .setName("jenkins-agent-0")
                    .setImage("jenkins/inbound-agent:latest-jdk21")
                    .addCommand("sh")
                    .addAllArgs(List.of("-c", "tail -f /dev/null"))
                    .setResources(ResourceRequirements.newBuilder()
                        .putLimits("memory", Quantity.newBuilder().setString("512Mi").build())
                        .putLimits("cpu", Quantity.newBuilder().setString("2").build())
                        .putRequests("memory", Quantity.newBuilder().setString("512Mi").build())
                        .putRequests("cpu", Quantity.newBuilder().setString("2").build())
                        .build())
                    .build())
                .build())
            .build())
        .build();

    JobSubmitResponse jobSubmitResponse = armadaClient.submitJob(jobSubmitRequest);

    String jobId = jobSubmitResponse.getJobResponseItems(0).getJobId();
    LOG.info("jobId: " + jobId);
  }

}
