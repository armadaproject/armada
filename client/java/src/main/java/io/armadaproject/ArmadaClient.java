package io.armadaproject;

import api.EventGrpc;
import api.EventOuterClass.EventStreamMessage;
import api.EventOuterClass.JobSetRequest;
import api.Health.HealthCheckResponse;
import api.Health.HealthCheckResponse.ServingStatus;
import api.Job.JobStatusRequest;
import api.Job.JobStatusResponse;
import api.JobsGrpc;
import api.SubmitGrpc;
import api.SubmitOuterClass.CancellationResult;
import api.SubmitOuterClass.JobCancelRequest;
import api.SubmitOuterClass.JobSubmitRequest;
import api.SubmitOuterClass.JobSubmitResponse;
import api.SubmitOuterClass.Queue;
import api.SubmitOuterClass.QueueGetRequest;
import api.SubmitOuterClass.StreamingQueueGetRequest;
import api.SubmitOuterClass.StreamingQueueMessage;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.util.Iterator;
import java.util.logging.Logger;

public class ArmadaClient {

  private static final Logger LOG = Logger.getLogger(ArmadaClient.class.getName());
  private final ManagedChannel channel;

  public ArmadaClient(String host, int port) {
    channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
  }

  public ArmadaClient(String host, int port, String bearerToken) {
    channel = ManagedChannelBuilder
        .forAddress(host, port)
        .useTransportSecurity()
        .intercept(MetadataUtils.newAttachHeadersInterceptor(createAuthMetadata(bearerToken)))
        .build();
  }

  private Metadata createAuthMetadata(String token) {
    Metadata metadata = new Metadata();
    metadata.put(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER),
        "Bearer " + token);
    return metadata;
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

  public Queue getQueue(QueueGetRequest queueGetRequest) {
    SubmitGrpc.SubmitBlockingStub submitBlockingStub = SubmitGrpc.newBlockingStub(channel);
    return submitBlockingStub.getQueue(queueGetRequest);
  }

  public Iterator<StreamingQueueMessage> getQueueInfo(
      StreamingQueueGetRequest streamingQueueGetRequest) {
    SubmitGrpc.SubmitBlockingStub submitBlockingStub = SubmitGrpc.newBlockingStub(channel);
    return submitBlockingStub.getQueues(streamingQueueGetRequest);
  }

  public JobStatusResponse getJobStatus(JobStatusRequest jobStatusRequest) {
    JobsGrpc.JobsBlockingStub jobsBlockingStub = JobsGrpc.newBlockingStub(channel);
    return jobsBlockingStub.getJobStatus(jobStatusRequest);
  }

}
