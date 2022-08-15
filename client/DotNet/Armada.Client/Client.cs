using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace GResearch.Armada.Client
{
    public interface IEvent
    {
        string JobId { get; }
        string JobSetId { get;  }
        string Queue { get;  }
        System.DateTimeOffset? Created { get; }
    }
    
    public interface IArmadaClient
    {
        Task<ApiCancellationResult> CancelJobsAsync(ApiJobCancelRequest body);
        Task<ApiJobSubmitResponse> SubmitJobsAsync(ApiJobSubmitRequest body);
        Task<object> CreateQueueAsync(ApiQueue body);
        Task<object> UpdateQueueAsync(string name, ApiQueue body);
        Task<object> DeleteQueueAsync(string name);
        Task<ApiQueue> GetQueueAsync(string name);
        Task<IEnumerable<StreamResponse<ApiEventStreamMessage>>> GetJobEventsStream(string queue, string jobSetId, string fromMessage = null, bool watch = false);
        Task WatchEvents(
            string queue,
            string jobSetId,
            string fromMessageId, 
            CancellationToken ct,
            Action<StreamResponse<ApiEventStreamMessage>> onMessage, 
            Action<Exception> onException = null);
    }

    public partial class ApiEventMessage
    {
        public IEvent Event => Cancelled ?? Submitted ?? Queued ?? DuplicateFound ?? Leased ?? LeaseReturned ??
                               LeaseExpired ?? Pending ?? Running ?? UnableToSchedule ??
                               Failed ?? Succeeded ?? Reprioritized ?? Cancelling ?? Cancelled ?? Terminated ?? 
                               Utilisation ?? IngressInfo ?? Reprioritizing ?? Updated ?? FailedCompressed as IEvent;
    }

    public partial class ApiJobSubmittedEvent : IEvent {}
    public partial class ApiJobQueuedEvent : IEvent {}
    public partial class ApiJobDuplicateFoundEvent : IEvent {}
    public partial class ApiJobLeasedEvent : IEvent {}
    public partial class ApiJobLeaseReturnedEvent : IEvent {}
    public partial class ApiJobLeaseExpiredEvent : IEvent {}
    public partial class ApiJobPendingEvent : IEvent {}
    public partial class ApiJobRunningEvent : IEvent {}
    public partial class ApiJobUnableToScheduleEvent : IEvent {}
    public partial class ApiJobFailedEvent : IEvent {}
    public partial class ApiJobSucceededEvent : IEvent {}
    public partial class ApiJobReprioritizedEvent  : IEvent {}
    public partial class ApiJobCancellingEvent  : IEvent {}
    public partial class ApiJobCancelledEvent  : IEvent {}
    public partial class ApiJobTerminatedEvent : IEvent {}
    public partial class ApiJobUtilisationEvent : IEvent {}
    public partial class ApiJobIngressInfoEvent : IEvent {}
    public partial class ApiJobReprioritizingEvent : IEvent {}
    public partial class ApiJobUpdatedEvent : IEvent {}

    public partial class ApiJobSubmitRequestItem
    {
        public ApiJobSubmitRequestItem()
        {
            ClientId = Guid.NewGuid().ToString("N");
        }
    }

    public class StreamResponse<T>
    {
        public T Result { get; set; }
        public string Error { get; set; }
    }

    public partial class ArmadaClient : IArmadaClient
    {
        public async Task<IEnumerable<StreamResponse<ApiEventStreamMessage>>> GetJobEventsStream(
            string queue, string jobSetId, string fromMessageId = null, bool watch = false)
        {
            var fileResponse = await GetJobSetEventsCoreAsync(queue, jobSetId,
                new ApiJobSetRequest {FromMessageId = fromMessageId, Watch = watch});
            return ReadEventStream(fileResponse.Stream);
        }
        
        private IEnumerable<StreamResponse<ApiEventStreamMessage>> ReadEventStream(Stream stream)
        {
            using (var reader = new StreamReader(stream))
            {
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    
                    var (_, eventMessage) = ProcessEventLine(null, line);
                    if (eventMessage != null)
                    {
                        yield return eventMessage;
                    }
                }
            }
        }

        public async Task WatchEvents(
            string queue,
            string jobSetId, 
            string fromMessageId, 
            CancellationToken ct, 
            Action<StreamResponse<ApiEventStreamMessage>> onMessage,
            Action<Exception> onException = null)
        {
            var failCount = 0;
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    using (var fileResponse = await GetJobSetEventsCoreAsync(queue, jobSetId,
                        new ApiJobSetRequest {FromMessageId = fromMessageId, Watch = true}, ct))
                    using (var reader = new StreamReader(fileResponse.Stream))
                    {
                        try
                        {
                            failCount = 0;
                            while (!ct.IsCancellationRequested && !reader.EndOfStream)
                            {
                                var line = await reader.ReadLineAsync();
                                var (newMessageId, eventMessage) = ProcessEventLine(fromMessageId, line);
                                fromMessageId = newMessageId;
                                if (eventMessage != null)
                                {
                                    onMessage(eventMessage);
                                }
                            }
                        }
                        catch (IOException)
                        {
                            // Stream was probably closed by the server, continue to reconnect
                        }
                    }
                }
                catch (TaskCanceledException)
                {
                    // Server closed the connection, continue to reconnect
                }
                catch (Exception e)
                {
                    failCount++;
                    onException?.Invoke(e);
                    // gradually back off
                    await Task.Delay(TimeSpan.FromSeconds(Math.Min(300, Math.Pow(2 ,failCount))), ct);
                }
            }
        }
        
        private (string, StreamResponse<ApiEventStreamMessage>) ProcessEventLine(string fromMessageId, string line)
        {
            try
            {
                var eventMessage =
                    JsonConvert.DeserializeObject<StreamResponse<ApiEventStreamMessage>>(line,
                        this.JsonSerializerSettings);

                fromMessageId = eventMessage?.Result?.Id ?? fromMessageId;
                
                // Ignore unknown event types
                if (String.IsNullOrEmpty(eventMessage?.Error) &&
                    eventMessage?.Result?.Message?.Event == null)
                {
                    eventMessage = null;
                }
                return (fromMessageId, eventMessage);
            }
            catch(Exception)
            {
                // Ignore messages which can't be deserialized    
            }
            
            return (fromMessageId, null);
        }
    }
}

