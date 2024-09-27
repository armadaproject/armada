package armadaproject;

import api.EventGrpc;

public class Main {

  public static void main(String[] args) {
    EventGrpc.EventBlockingStub stub = EventGrpc.newBlockingStub(null);
  }

}
