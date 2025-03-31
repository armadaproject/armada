package io.armadaproject.examples;

import api.SubmitOuterClass.Queue;
import api.SubmitOuterClass.QueueGetRequest;
import io.armadaproject.ArmadaClient;
import java.util.logging.Logger;

public class GetQueue {

  private static final Logger LOG = Logger.getLogger(GetQueue.class.getName());

  public static void main(String[] args) {
    ArmadaClient armadaClient = new ArmadaClient("localhost", 30002);

    Queue queue = armadaClient.getQueue(QueueGetRequest.newBuilder()
        .setName("example")
        .build());

    LOG.info("queue:" + queue);
  }

}
