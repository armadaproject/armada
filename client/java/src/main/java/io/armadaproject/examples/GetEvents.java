package io.armadaproject.examples;

import api.EventOuterClass.JobSetRequest;
import io.armadaproject.ArmadaClient;
import java.util.logging.Logger;

public class GetEvents {

  private static final Logger LOG = Logger.getLogger(GetEvents.class.getName());

  public static void main(String[] args) {
    ArmadaClient armadaClient = new ArmadaClient("localhost", 30002);

    JobSetRequest jobSetRequest = JobSetRequest.newBuilder()
        .setId("example")
        .setQueue("example")
        .setErrorIfMissing(true)
        .build();

    armadaClient.getEvents(jobSetRequest).forEachRemaining(e -> {
      LOG.info("event:" + e);
    });
  }

}
