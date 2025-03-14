package io.armadaproject;

import api.Health.HealthCheckResponse.ServingStatus;
import api.Job.JobStatusRequest;
import api.Job.JobStatusResponse;
import api.SubmitOuterClass;
import api.SubmitOuterClass.*;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class ArmadaClientTest {
    private static final String ARMADA_CLIENT_HOST = "localhost";
    private static final int TEST_PORT = 12345;
    private static ArmadaClient armadaClient;
    private static MockServer mockServer;


    @BeforeAll
    public static void setUp() throws IOException {
        mockServer = new MockServer(TEST_PORT);
        mockServer.start();
        armadaClient = new ArmadaClient(ARMADA_CLIENT_HOST, TEST_PORT);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        mockServer.stop();
        armadaClient.close();
    }

    @Test
    public void testCheckHealth() {
        ServingStatus status = armadaClient.checkHealth();
        assertEquals(ServingStatus.SERVING, status);
    }

    @Test
    public void testSubmitJobs() {
        String jobSetId = "test-job-set";
        String queueName = "example";

        JobSubmitRequest submitRequest = JobSubmitRequest.newBuilder()
                .setQueue(queueName)
                .setJobSetId(jobSetId)
                .addJobRequestItems(JobSubmitRequestItem.newBuilder().build())
                .build();

        JobSubmitResponse submitResponse = armadaClient.submitJob(submitRequest);
        assertNotNull(submitResponse);
        assertEquals(1, submitResponse.getJobResponseItemsCount());

        String jobId = submitResponse.getJobResponseItems(0).getJobId();

        // Verify the job status
        JobStatusRequest statusRequest = JobStatusRequest.newBuilder().addJobIds(jobId).build();
        JobStatusResponse statusResponse = armadaClient.getJobStatus(statusRequest);
        assertNotNull(statusResponse);
        assertEquals(SubmitOuterClass.JobState.RUNNING, statusResponse.getJobStatesMap().get(jobId));
    }

    @Test
    public void testCancelJobs() {
        String jobSetId = "test-job-set";
        String queueName = "example";

        JobSubmitRequest submitRequest = JobSubmitRequest.newBuilder()
                .setQueue(queueName)
                .setJobSetId(jobSetId)
                .addJobRequestItems(JobSubmitRequestItem.newBuilder().build())
                .build();

        JobSubmitResponse submitResponse = armadaClient.submitJob(submitRequest);
        String jobId = submitResponse.getJobResponseItems(0).getJobId();

        // Cancel the job
        armadaClient.cancelJob(JobCancelRequest.newBuilder().addJobIds(jobId).build());

        // job status should be CANCELLED
        JobStatusRequest statusRequest = JobStatusRequest.newBuilder().addJobIds(jobId).build();
        JobStatusResponse statusResponse = armadaClient.getJobStatus(statusRequest);
        assertEquals(SubmitOuterClass.JobState.CANCELLED, statusResponse.getJobStatesMap().get(jobId));
    }

    @Test
    public void testGetQueue() {
        String queueName = "example";
        QueueGetRequest getRequest = QueueGetRequest.newBuilder().setName(queueName).build();
        Queue retrievedQueue = armadaClient.getQueue(getRequest);
        assertEquals(queueName, retrievedQueue.getName());
    }

    @Test
    public void testSubmitJobsToNonExistentQueue() {
        String nonExistentQueueName = "non-existent-queue";

        JobSubmitRequest submitRequest = JobSubmitRequest.newBuilder()
                .setQueue(nonExistentQueueName)
                .setJobSetId("test-job-set")
                .addJobRequestItems(JobSubmitRequestItem.newBuilder().build())
                .build();

        assertThrows(StatusRuntimeException.class, () -> armadaClient.submitJob(submitRequest));
    }
}