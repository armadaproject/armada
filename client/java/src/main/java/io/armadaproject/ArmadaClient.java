package io.armadaproject;

import api.Health.HealthCheckResponse;
import api.QueueServiceGrpc;
import api.SubmitGrpc;
import api.SubmitOuterClass.Queue;
import api.SubmitOuterClass.QueueGetRequest;
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
    LOG.info("connection to armada server is " + healthCheckResponse.getStatus());
  }

  public void createQueue(String queueName) {
    QueueServiceGrpc.QueueServiceBlockingStub queueServiceBlockingStub =
        QueueServiceGrpc.newBlockingStub(channel);
    queueServiceBlockingStub.createQueue(Queue.newBuilder()
        .setPriorityFactor(1.00d)
        .setName(queueName).build());
    LOG.info("queue created");
  }

  public Queue getQueue(String queueName) {
    QueueServiceGrpc.QueueServiceBlockingStub queueServiceBlockingStub =
        QueueServiceGrpc.newBlockingStub(channel);
    return queueServiceBlockingStub.getQueue(
        QueueGetRequest.newBuilder().setName(queueName).build());
  }

  public void submitJob(String queueName) {
    SubmitGrpc.SubmitBlockingStub submitBlockingStub = SubmitGrpc.newBlockingStub(channel);
  }

}
