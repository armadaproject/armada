package io.armadaproject.examples;

import api.Job.JobStatusRequest;
import api.Job.JobStatusResponse;
import io.armadaproject.ArmadaClient;
import java.util.logging.Logger;

public class GetJobStatus {

  private static final Logger LOG = Logger.getLogger(GetJobStatus.class.getName());

  public static void main(String[] args) {
    ArmadaClient armadaClient = new ArmadaClient("localhost", 30002);

    JobStatusResponse jobStatus = armadaClient.getJobStatus(JobStatusRequest.newBuilder()
        .addJobIds("01je8sr94msnkcwvne7f41cynv")
        .build());

    LOG.info("job status: " + jobStatus);
  }

}
