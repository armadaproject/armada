package io.armadaproject;

import api.Health.HealthCheckResponse;
import api.SubmitGrpc;
import api.SubmitOuterClass.JobSubmitRequest;
import api.SubmitOuterClass.JobSubmitResponse;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.logging.Logger;

public class ArmadaClient {

  private static final Logger LOG = Logger.getLogger(ArmadaClient.class.getName());
  private final ManagedChannel channel;

  // TODO: add security
  public ArmadaClient(String host, int port) {
    channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
  }

  public void checkHealth() {
    LOG.info("checking connection to armada server...");
    SubmitGrpc.SubmitBlockingStub submitBlockingStub = SubmitGrpc.newBlockingStub(channel);
    HealthCheckResponse healthCheckResponse = submitBlockingStub.health(Empty.getDefaultInstance());
    LOG.info("connection to armada server: " + healthCheckResponse.getStatus());
  }

  public JobSubmitResponse submitJob(JobSubmitRequest jobSubmitRequest) {
    SubmitGrpc.SubmitBlockingStub submitBlockingStub = SubmitGrpc.newBlockingStub(channel);
    JobSubmitResponse jobSubmitResponse = submitBlockingStub.submitJobs(jobSubmitRequest);
    LOG.info("job submitted! response: \n" + jobSubmitResponse);
    return jobSubmitResponse;
  }

}
