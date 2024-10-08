package io.armadaproject;

import api.EventOuterClass.JobSetRequest;
import java.util.logging.Logger;

public class EventExample {

  private static final Logger LOG = Logger.getLogger(EventExample.class.getName());

  public static void main(String[] args) {
    ArmadaClient armadaClient = new ArmadaClient("localhost", 30002);

    JobSetRequest jobSetRequest = JobSetRequest.newBuilder()
        .setId("example")
        .setQueue("example")
        .setErrorIfMissing(true)
        .build();
    armadaClient.getEvents(jobSetRequest).forEachRemaining(e -> LOG.info("event:" + e));
  }

}
