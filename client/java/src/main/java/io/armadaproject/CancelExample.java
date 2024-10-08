package io.armadaproject;

import api.SubmitOuterClass.CancellationResult;
import api.SubmitOuterClass.JobCancelRequest;
import java.util.logging.Logger;

public class CancelExample {

  private static final Logger LOG = Logger.getLogger(CancelExample.class.getName());

  public static void main(String[] args) {
    ArmadaClient armadaClient = new ArmadaClient("localhost", 30002);

    String queueName = "example";
    // TODO use @{link SubmitExample} to submit a job and get the job id
    String jobId = "01j9p0q536g8s1zrghhv38mc8q";
    JobCancelRequest jobCancelRequest = JobCancelRequest.newBuilder()
        .setJobId(jobId)
        .setQueue(queueName)
        .setJobSetId(queueName)
        .build();
    CancellationResult cancellationResult = armadaClient.cancelJob(jobCancelRequest);
    LOG.info("num of jobs cancelled: " + cancellationResult.getCancelledIdsCount());
  }

}
