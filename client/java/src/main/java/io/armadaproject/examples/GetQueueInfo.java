package io.armadaproject.examples;

import api.SubmitOuterClass.StreamingQueueGetRequest;
import io.armadaproject.ArmadaClient;
import java.util.logging.Logger;

public class GetQueueInfo {

  private static final Logger LOG = Logger.getLogger(GetQueueInfo.class.getName());

  public static void main(String[] args) {
    ArmadaClient armadaClient = new ArmadaClient("localhost", 30002);

    armadaClient.getQueueInfo(StreamingQueueGetRequest.newBuilder().build()).forEachRemaining(q -> {
      LOG.info("queue:" + q);
    });
  }

}
