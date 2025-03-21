package io.armadaproject.examples;

import api.Health.HealthCheckResponse.ServingStatus;
import io.armadaproject.ArmadaClient;

public class CheckConnectionExample {

  public static void main(String[] args) {
    ArmadaClient armadaClient = new ArmadaClient("localhost", 30002);

    if (armadaClient.checkHealth() != ServingStatus.SERVING) {
      throw new RuntimeException("armada server is not serving");
    }
  }

}
