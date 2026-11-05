using System;
using System.Net.Http;
using System.Threading.Tasks;
using ArmadaProject.Io.Api;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using NUnit.Framework;

namespace ArmadaProject.Io.Client.Test;

[TestFixture]
public class Tests
{
    private static MockServer _mockServer = null!;
    private static GrpcChannel _channel = null!;
    private static Submit.SubmitClient _submitClient = null!;
    private static Event.EventClient _eventClient = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _mockServer = new MockServer();

        var handler = _mockServer.CreateHandler();

        _channel = GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions
        {
            HttpClient = new HttpClient(handler),
            DisposeHttpClient = true
        });
        _submitClient = new Submit.SubmitClient(_channel);
        _eventClient = new Event.EventClient(_channel);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        _channel.ShutdownAsync().Wait();
        _mockServer.Dispose();
    }

    /// <summary>
    /// Test health check returns SERVING status.
    /// Matches Java: testCheckHealth()
    /// </summary>
    [Test]
    public void TestCheckHealth()
    {
        var response = _submitClient.Health(new Empty());
        Assert.That(response.Status, Is.EqualTo(HealthCheckResponse.Types.ServingStatus.Serving));
    }

    /// <summary>
    /// Test job submission returns valid response with job ID.
    /// Matches Java: testSubmitJobs()
    /// </summary>
    [Test]
    public async Task TestSubmitJobs()
    {
        var jobSetId = "test-job-set";
        var queueName = "example";

        var submitRequest = new JobSubmitRequest
        {
            Queue = queueName,
            JobSetId = jobSetId,
        };
        submitRequest.JobRequestItems.Add(new JobSubmitRequestItem());

        var submitResponse = await _submitClient.SubmitJobsAsync(submitRequest);

        Assert.That(submitResponse, Is.Not.Null);
        Assert.That(submitResponse.JobResponseItems.Count, Is.EqualTo(1));

        var jobId = submitResponse.JobResponseItems[0].JobId;
        Assert.That(jobId, Is.Not.Null.And.Not.Empty);
    }

    /// <summary>
    /// Test job cancellation works.
    /// Matches Java: testCancelJobs()
    /// Note: Cannot verify status change via Jobs service as job.proto is excluded from DotNet client.
    /// </summary>
    [Test]
    public async Task TestCancelJobs()
    {
        var jobSetId = "test-job-set";
        var queueName = "example";

        var submitRequest = new JobSubmitRequest
        {
            Queue = queueName,
            JobSetId = jobSetId,
        };
        submitRequest.JobRequestItems.Add(new JobSubmitRequestItem());

        var submitResponse = await _submitClient.SubmitJobsAsync(submitRequest);
        var jobId = submitResponse.JobResponseItems[0].JobId;

        // Cancel the job (same as Java test)
        var cancelRequest = new JobCancelRequest();
        cancelRequest.JobIds.Add(jobId);
        var cancelResponse = await _submitClient.CancelJobsAsync(cancelRequest);

        // Verify the job was cancelled (returned in cancelled_ids)
        Assert.That(cancelResponse.CancelledIds, Does.Contain(jobId));
    }

    /// <summary>
    /// Test queue retrieval returns correct queue name.
    /// Matches Java: testGetQueue()
    /// </summary>
    [Test]
    public void TestGetQueue()
    {
        var queueName = "example";
        var getRequest = new QueueGetRequest { Name = queueName };

        var retrievedQueue = _submitClient.GetQueue(getRequest);

        Assert.That(retrievedQueue.Name, Is.EqualTo(queueName));
    }

    /// <summary>
    /// Test error handling - submitting to non-existent queue throws exception.
    /// Matches Java: testSubmitJobsToNonExistentQueue()
    /// </summary>
    [Test]
    public void TestSubmitJobsToNonExistentQueue()
    {
        var nonExistentQueueName = "non-existent-queue";

        var submitRequest = new JobSubmitRequest
        {
            Queue = nonExistentQueueName,
            JobSetId = "test-job-set",
        };
        submitRequest.JobRequestItems.Add(new JobSubmitRequestItem());

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await _submitClient.SubmitJobsAsync(submitRequest)
        );

        Assert.That(ex, Is.Not.Null);
        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.NotFound));
    }

    /// <summary>
    /// Test event health check returns SERVING status.
    /// Matches Python: test_health_event()
    /// </summary>
    [Test]
    public void TestEventHealth()
    {
        var response = _eventClient.Health(new Empty());
        Assert.That(response.Status, Is.EqualTo(HealthCheckResponse.Types.ServingStatus.Serving));
    }

    /// <summary>
    /// Test reading the job set event stream returns the populated event and then completes.
    /// Matches Python: test_get_job_events_stream()
    /// </summary>
    [Test]
    public async Task TestGetJobSetEvents()
    {
        var jobSetId = "test-job-set";
        var queueName = "example";

        using var call = _eventClient.GetJobSetEvents(new JobSetRequest
        {
            Id = jobSetId,
            Queue = queueName
        });

        Assert.That(await call.ResponseStream.MoveNext(), Is.True);
        var streamMessage = call.ResponseStream.Current;
        Assert.That(streamMessage.Id, Is.EqualTo(MockEventService.EventId));
        Assert.That(streamMessage.Message.EventsCase, Is.EqualTo(EventMessage.EventsOneofCase.Submitted));
        Assert.That(streamMessage.Message.Submitted.JobId, Is.EqualTo(MockEventService.JobId));
        Assert.That(streamMessage.Message.Submitted.JobSetId, Is.EqualTo(jobSetId));
        Assert.That(streamMessage.Message.Submitted.Queue, Is.EqualTo(queueName));

        Assert.That(await call.ResponseStream.MoveNext(), Is.False);
        Assert.That(call.GetStatus().StatusCode, Is.EqualTo(StatusCode.OK));
    }
}
