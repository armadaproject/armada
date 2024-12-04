package io.armadaproject.examples;

import api.SubmitOuterClass.CancellationResult;
import api.SubmitOuterClass.JobCancelRequest;
import io.armadaproject.ArmadaClient;
import java.util.logging.Logger;

public class CancelJob {

  private static final Logger LOG = Logger.getLogger(CancelJob.class.getName());

  public static void main(String[] args) {
    ArmadaClient armadaClient = new ArmadaClient("localhost", 30002);

    String queueName = "example";
    // TODO use @{link SubmitExample} to submit a job and get the job id
    String jobId = "01je8sr94msnkcwvne7f41cynv";
    JobCancelRequest jobCancelRequest = JobCancelRequest.newBuilder()
        .setJobId(jobId)
        .setQueue(queueName)
        .setJobSetId(queueName)
        .build();
    CancellationResult cancellationResult = armadaClient.cancelJob(jobCancelRequest);
    LOG.info("num of jobs cancelled: " + cancellationResult.getCancelledIdsCount());
  }

}
