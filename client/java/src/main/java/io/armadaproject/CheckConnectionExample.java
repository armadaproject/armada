package io.armadaproject;

import api.Health.HealthCheckResponse.ServingStatus;

public class CheckConnectionExample {

  public static void main(String[] args) {
    ArmadaClient armadaClient = new ArmadaClient("localhost", 30002);

    if (armadaClient.checkHealth() != ServingStatus.SERVING) {
      throw new RuntimeException("armada server is not serving");
    }
  }

}
