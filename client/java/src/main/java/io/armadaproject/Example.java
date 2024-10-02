package io.armadaproject;

import java.util.UUID;
import java.util.logging.Logger;

public class Example {

  private static final Logger LOG = Logger.getLogger(Example.class.getName());

  public static void main(String[] args) {
    ArmadaClient armadaClient = new ArmadaClient("localhost", 30002);
    armadaClient.checkHealth();
    String queueName = UUID.randomUUID().toString();
    armadaClient.createQueue(queueName);
    LOG.info("queue: " + armadaClient.getQueue(queueName));
  }

}
