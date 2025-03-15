package io.armadaproject;

import api.*;
import com.google.protobuf.Empty;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class MockServer {
    private final int port;
    private Server server;

    // Stateful maps for queues, jobs, and job statuses
    private final ConcurrentHashMap<String, SubmitOuterClass.Queue> queueMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SubmitOuterClass.Job> jobMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SubmitOuterClass.JobState> statusMap = new ConcurrentHashMap<>();

    private static final String DEFAULT_QUEUE = "example";

    public MockServer(int port) {
        this.port = port;
        // Initialize the queueMap with the fixed queue "example"
        queueMap.put(DEFAULT_QUEUE, SubmitOuterClass.Queue.newBuilder().setName(DEFAULT_QUEUE).build());
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MockEventService())
                .addService(new MockSubmitService(queueMap, jobMap, statusMap))
                .addService(new MockJobsService(statusMap))
                .build()
                .start();
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private static class MockEventService extends EventGrpc.EventImplBase {
        @Override
        public void getJobSetEvents(EventOuterClass.JobSetRequest request, StreamObserver<EventOuterClass.EventStreamMessage> responseObserver) {
            responseObserver.onNext(EventOuterClass.EventStreamMessage.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }

    private static class MockSubmitService extends SubmitGrpc.SubmitImplBase {
        private final ConcurrentHashMap<String, SubmitOuterClass.Queue> queueMap;
        private final ConcurrentHashMap<String, SubmitOuterClass.Job> jobMap;
        private final ConcurrentHashMap<String, SubmitOuterClass.JobState> statusMap;

        public MockSubmitService(
                ConcurrentHashMap<String, SubmitOuterClass.Queue> queueMap,
                ConcurrentHashMap<String, SubmitOuterClass.Job> jobMap,
                ConcurrentHashMap<String, SubmitOuterClass.JobState> statusMap) {
            this.queueMap = queueMap;
            this.jobMap = jobMap;
            this.statusMap = statusMap;
        }

        @Override
        public void health(Empty request, StreamObserver<Health.HealthCheckResponse> responseObserver) {
            responseObserver.onNext(Health.HealthCheckResponse.newBuilder()
                    .setStatus(Health.HealthCheckResponse.ServingStatus.SERVING)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void submitJobs(SubmitOuterClass.JobSubmitRequest request, StreamObserver<SubmitOuterClass.JobSubmitResponse> responseObserver) {
            String queueName = request.getQueue();

            // Check if the queue exists
            if (!queueMap.containsKey(queueName)) {
                responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription("Queue not found")));
                return;
            }

            String jobId = "job-" + System.currentTimeMillis();
            SubmitOuterClass.Job job = SubmitOuterClass.Job.newBuilder()
                    .setJobSetId(request.getJobSetId())
                    .build();
            jobMap.put(jobId, job);
            statusMap.put(jobId, SubmitOuterClass.JobState.RUNNING);

            SubmitOuterClass.JobSubmitResponse response = SubmitOuterClass.JobSubmitResponse.newBuilder()
                    .addJobResponseItems(SubmitOuterClass.JobSubmitResponseItem.newBuilder().setJobId(jobId).build()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getQueue(SubmitOuterClass.QueueGetRequest request, StreamObserver<SubmitOuterClass.Queue> responseObserver) {
            // Return the queue if it exists in the queueMap
            SubmitOuterClass.Queue queue = queueMap.get(request.getName());
            if (queue != null) {
                responseObserver.onNext(queue);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription("Queue not found")));
            }
        }

        @Override
        public void cancelJobs(SubmitOuterClass.JobCancelRequest request, StreamObserver<SubmitOuterClass.CancellationResult> responseObserver) {
            SubmitOuterClass.CancellationResult.Builder resultBuilder = SubmitOuterClass.CancellationResult.newBuilder();

            // Cancel each job in the request
            for (String jobId : request.getJobIdsList()) {
                if (statusMap.containsKey(jobId)) {
                    statusMap.put(jobId, SubmitOuterClass.JobState.CANCELLED);
                    resultBuilder.addCancelledIds(jobId);
                }
            }

            // Return the cancellation result
            responseObserver.onNext(resultBuilder.build());
            responseObserver.onCompleted();
        }
    }

    private static class MockJobsService extends JobsGrpc.JobsImplBase {
        private final ConcurrentHashMap<String, SubmitOuterClass.JobState> statusMap;

        public MockJobsService(ConcurrentHashMap<String, SubmitOuterClass.JobState> statusMap) {
            this.statusMap = statusMap;
        }

        @Override
        public void getJobStatus(Job.JobStatusRequest request, StreamObserver<Job.JobStatusResponse> responseObserver) {
            Job.JobStatusResponse.Builder responseBuilder = Job.JobStatusResponse.newBuilder();
            for (String jobId : request.getJobIdsList()) {
                SubmitOuterClass.JobState state = statusMap.get(jobId);
                if (state != null) {
                    responseBuilder.putJobStates(jobId, state);
                }
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }
    }
}