using System;
using System.Collections.Concurrent;
using System.Net.Http;
using System.Threading.Tasks;
using ArmadaProject.Io.Api;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace ArmadaProject.Io.Client.Test;

/// <summary>
/// Builds gRPC method descriptors for the mock services.
/// The client package only generates client stubs, so the server side of each RPC is bound by
/// hand in the *ServiceBase classes below. Message types come from the client assembly.
/// </summary>
internal static class Rpc
{
    public static Method<TRequest, TResponse> Unary<TRequest, TResponse>(string service, string name)
        where TRequest : IMessage<TRequest>, new()
        where TResponse : IMessage<TResponse>, new()
        => new(MethodType.Unary, service, name, Marshaller<TRequest>(), Marshaller<TResponse>());

    public static Method<TRequest, TResponse> ServerStreaming<TRequest, TResponse>(string service, string name)
        where TRequest : IMessage<TRequest>, new()
        where TResponse : IMessage<TResponse>, new()
        => new(MethodType.ServerStreaming, service, name, Marshaller<TRequest>(), Marshaller<TResponse>());

    private static Marshaller<T> Marshaller<T>() where T : IMessage<T>, new()
    {
        var parser = new MessageParser<T>(() => new T());
        return Marshallers.Create(message => message.ToByteArray(), parser.ParseFrom);
    }

    public static RpcException Unimplemented() => new(new Status(StatusCode.Unimplemented, ""));
}

/// <summary>
/// Hand-written equivalent of the protoc-generated Submit.SubmitBase, limited to the RPCs the
/// mock implements. Methods are virtual because ASP.NET Core binds each RPC to the override of
/// the method of the same name on the type passed to BindService.
/// </summary>
[BindServiceMethod(typeof(SubmitServiceBase), nameof(BindService))]
public abstract class SubmitServiceBase
{
    private const string ServiceName = "api.Submit";

    // ASP.NET Core calls this with a null service while discovering endpoints and resolves the
    // handler for each RPC by method name, so the delegates are only created for a real instance
    // (this mirrors the protoc-generated BindService).
    public static void BindService(ServiceBinderBase binder, SubmitServiceBase service)
    {
        binder.AddMethod(Rpc.Unary<Empty, HealthCheckResponse>(ServiceName, "Health"), service is null ? null : service.Health);
        binder.AddMethod(Rpc.Unary<JobSubmitRequest, JobSubmitResponse>(ServiceName, "SubmitJobs"), service is null ? null : service.SubmitJobs);
        binder.AddMethod(Rpc.Unary<QueueGetRequest, Queue>(ServiceName, "GetQueue"), service is null ? null : service.GetQueue);
        binder.AddMethod(Rpc.Unary<JobCancelRequest, CancellationResult>(ServiceName, "CancelJobs"), service is null ? null : service.CancelJobs);
        binder.AddMethod(Rpc.Unary<Queue, Empty>(ServiceName, "CreateQueue"), service is null ? null : service.CreateQueue);
        binder.AddMethod(Rpc.Unary<QueueDeleteRequest, Empty>(ServiceName, "DeleteQueue"), service is null ? null : service.DeleteQueue);
    }

    public virtual Task<HealthCheckResponse> Health(Empty request, ServerCallContext context) => throw Rpc.Unimplemented();
    public virtual Task<JobSubmitResponse> SubmitJobs(JobSubmitRequest request, ServerCallContext context) => throw Rpc.Unimplemented();
    public virtual Task<Queue> GetQueue(QueueGetRequest request, ServerCallContext context) => throw Rpc.Unimplemented();
    public virtual Task<CancellationResult> CancelJobs(JobCancelRequest request, ServerCallContext context) => throw Rpc.Unimplemented();
    public virtual Task<Empty> CreateQueue(Queue request, ServerCallContext context) => throw Rpc.Unimplemented();
    public virtual Task<Empty> DeleteQueue(QueueDeleteRequest request, ServerCallContext context) => throw Rpc.Unimplemented();
}

/// <summary>
/// Hand-written equivalent of the protoc-generated Event.EventBase, limited to the RPCs the
/// mock implements.
/// </summary>
[BindServiceMethod(typeof(EventServiceBase), nameof(BindService))]
public abstract class EventServiceBase
{
    private const string ServiceName = "api.Event";

    public static void BindService(ServiceBinderBase binder, EventServiceBase service)
    {
        binder.AddMethod(Rpc.ServerStreaming<JobSetRequest, EventStreamMessage>(ServiceName, "GetJobSetEvents"), service is null ? null : service.GetJobSetEvents);
        binder.AddMethod(Rpc.Unary<Empty, HealthCheckResponse>(ServiceName, "Health"), service is null ? null : service.Health);
    }

    public virtual Task GetJobSetEvents(JobSetRequest request, IServerStreamWriter<EventStreamMessage> responseStream, ServerCallContext context) => throw Rpc.Unimplemented();
    public virtual Task<HealthCheckResponse> Health(Empty request, ServerCallContext context) => throw Rpc.Unimplemented();
}

/// <summary>
/// Mock gRPC service implementations for testing.
/// </summary>
public class MockSubmitService : SubmitServiceBase
{
    private readonly ConcurrentDictionary<string, Queue> _queueMap;
    private readonly ConcurrentDictionary<string, Job> _jobMap;
    private readonly ConcurrentDictionary<string, JobState> _statusMap;

    private const string DefaultQueue = "example";

    public MockSubmitService()
    {
        _queueMap = new ConcurrentDictionary<string, Queue>();
        _jobMap = new ConcurrentDictionary<string, Job>();
        _statusMap = new ConcurrentDictionary<string, JobState>();

        // Initialize with the default queue (same as Java MockServer)
        _queueMap.TryAdd(DefaultQueue, new Queue { Name = DefaultQueue });
    }

    public override Task<HealthCheckResponse> Health(Empty request, ServerCallContext context)
    {
        return Task.FromResult(new HealthCheckResponse
        {
            Status = HealthCheckResponse.Types.ServingStatus.Serving
        });
    }

    public override Task<JobSubmitResponse> SubmitJobs(JobSubmitRequest request, ServerCallContext context)
    {
        var queueName = request.Queue;

        // Check if the queue exists (same as Java MockServer)
        if (!_queueMap.ContainsKey(queueName))
        {
            throw new RpcException(new Status(StatusCode.NotFound, "Queue not found"));
        }

        var jobId = $"job-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
        var job = new Job
        {
            JobSetId = request.JobSetId
        };
        _jobMap.TryAdd(jobId, job);
        _statusMap.TryAdd(jobId, JobState.Running);

        var response = new JobSubmitResponse();
        response.JobResponseItems.Add(new JobSubmitResponseItem { JobId = jobId });
        return Task.FromResult(response);
    }

    public override Task<Queue> GetQueue(QueueGetRequest request, ServerCallContext context)
    {
        if (_queueMap.TryGetValue(request.Name, out var queue))
        {
            return Task.FromResult(queue);
        }

        throw new RpcException(new Status(StatusCode.NotFound, "Queue not found"));
    }

    public override Task<CancellationResult> CancelJobs(JobCancelRequest request, ServerCallContext context)
    {
        var result = new CancellationResult();

        // Cancel each job in the request (same as Java MockServer)
        foreach (var jobId in request.JobIds)
        {
            if (_statusMap.ContainsKey(jobId))
            {
                _statusMap[jobId] = JobState.Cancelled;
                result.CancelledIds.Add(jobId);
            }
        }

        return Task.FromResult(result);
    }

    public override Task<Empty> CreateQueue(Queue request, ServerCallContext context)
    {
        _queueMap.TryAdd(request.Name, request);
        return Task.FromResult(new Empty());
    }

    public override Task<Empty> DeleteQueue(QueueDeleteRequest request, ServerCallContext context)
    {
        _queueMap.TryRemove(request.Name, out _);
        return Task.FromResult(new Empty());
    }
}

/// <summary>
/// Mock Event service - minimal implementation for testing
/// </summary>
public class MockEventService : EventServiceBase
{
    public override Task GetJobSetEvents(JobSetRequest request, IServerStreamWriter<EventStreamMessage> responseStream, ServerCallContext context)
    {
        return responseStream.WriteAsync(new EventStreamMessage());
    }

    public override Task<HealthCheckResponse> Health(Empty request, ServerCallContext context)
    {
        return Task.FromResult(new HealthCheckResponse
        {
            Status = HealthCheckResponse.Types.ServingStatus.Serving
        });
    }
}

/// <summary>
/// Mock gRPC server for testing.
/// Uses TestServer for in-process testing.
/// </summary>
public class MockServer : IDisposable
{
    private readonly WebApplication _app;
    private readonly MockSubmitService _submitService;
    private readonly MockEventService _eventService;

    public MockServer()
    {
        _submitService = new MockSubmitService();
        _eventService = new MockEventService();

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddSingleton(_submitService);
        builder.Services.AddSingleton(_eventService);
        builder.Services.AddGrpc();

        _app = builder.Build();
        _app.MapGrpcService<MockSubmitService>();
        _app.MapGrpcService<MockEventService>();
        _app.Start();
    }

    public HttpMessageHandler CreateHandler() => _app.GetTestServer().CreateHandler();

    public void Dispose()
    {
        _app.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
