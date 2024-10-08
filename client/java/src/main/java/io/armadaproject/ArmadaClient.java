package io.armadaproject;

import api.EventGrpc;
import api.EventOuterClass.EventStreamMessage;
import api.EventOuterClass.JobSetRequest;
import api.Health.HealthCheckResponse;
import api.Health.HealthCheckResponse.ServingStatus;
import api.SubmitGrpc;
import api.SubmitOuterClass.CancellationResult;
import api.SubmitOuterClass.JobCancelRequest;
import api.SubmitOuterClass.JobSubmitRequest;
import api.SubmitOuterClass.JobSubmitResponse;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Iterator;
import java.util.logging.Logger;

public class ArmadaClient {

  private static final Logger LOG = Logger.getLogger(ArmadaClient.class.getName());
  private final ManagedChannel channel;

  // TODO: add security
  public ArmadaClient(String host, int port) {
    channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
  }

  public ServingStatus checkHealth() {
    LOG.info("checking connection to armada server...");
    SubmitGrpc.SubmitBlockingStub submitBlockingStub = SubmitGrpc.newBlockingStub(channel);
    HealthCheckResponse healthCheckResponse = submitBlockingStub.health(Empty.getDefaultInstance());
    ServingStatus status = healthCheckResponse.getStatus();
    LOG.info("connection to armada server: " + status);
    return status;
  }

  public JobSubmitResponse submitJob(JobSubmitRequest jobSubmitRequest) {
    SubmitGrpc.SubmitBlockingStub submitBlockingStub = SubmitGrpc.newBlockingStub(channel);
    JobSubmitResponse jobSubmitResponse = submitBlockingStub.submitJobs(jobSubmitRequest);
    LOG.info("job submitted! response: \n" + jobSubmitResponse);
    return jobSubmitResponse;
  }

  public CancellationResult cancelJob(JobCancelRequest jobCancelRequest) {
    SubmitGrpc.SubmitBlockingStub submitBlockingStub = SubmitGrpc.newBlockingStub(channel);
    CancellationResult cancellationResult = submitBlockingStub.cancelJobs(jobCancelRequest);
    LOG.info("job cancelled! response: \n" + cancellationResult);
    return cancellationResult;
  }

  public Iterator<EventStreamMessage> getEvents(JobSetRequest jobSetRequest) {
    EventGrpc.EventBlockingStub eventBlockingStub = EventGrpc.newBlockingStub(channel);
    return eventBlockingStub.getJobSetEvents(jobSetRequest);
  }

}
