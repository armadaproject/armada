package io.armadaproject.examples;

import api.EventOuterClass.EventStreamMessage;
import api.EventOuterClass.JobSetRequest;
import io.armadaproject.ArmadaClient;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StreamEvents {

  private static final Logger LOG = Logger.getLogger(StreamEvents.class.getName());

  static {
    // Set the log level to FINE
    LOG.setLevel(Level.FINE);

    // Configure the console handler to output logs at FINE level
    ConsoleHandler consoleHandler = new ConsoleHandler();
    consoleHandler.setLevel(Level.FINE);
    LOG.addHandler(consoleHandler);
  }

  public static void main(String[] args) {
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    try (ArmadaClient armadaClient = new ArmadaClient("localhost", 30002)) {

      JobSetRequest jobSetRequest = JobSetRequest.newBuilder()
          .setId("job-set-1")
          .setQueue("example")
          .setWatch(true)
          .build();

      executorService.submit(() -> {
        StreamObserver<EventStreamMessage> streamObserver = new StreamObserver<>() {
          @Override
          public void onNext(EventStreamMessage value) {
            LOG.log(Level.FINE, "event: " + value);
          }

          @Override
          public void onError(Throwable t) {
            LOG.log(Level.SEVERE, "error: " + t);
          }

          @Override
          public void onCompleted() {
            LOG.info("completed");
          }
        };

        armadaClient.streamEvents(jobSetRequest, streamObserver);
      });

      // Keep the main thread alive to continue receiving events
      try {
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } catch (Exception e) {
      LOG.severe("Failed to close client: " + e);
    } finally {
      executorService.shutdown();
    }
  }
}