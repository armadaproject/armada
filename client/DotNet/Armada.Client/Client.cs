using System.Collections.Generic;
using System.IO;
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

    public partial class ApiEventMessage
    {
        public IEvent Event => Cancelled ?? Submitted ?? Queued ?? Leased ?? LeaseReturned ??
                               LeaseExpired ?? Pending ?? Running ?? UnableToSchedule ??
                               Failed ?? Succeeded ?? Reprioritized ?? Cancelling ?? Cancelled ?? Terminated as IEvent;
    }

    public partial class ApiJobSubmittedEvent : IEvent {}
    public partial class ApiJobQueuedEvent : IEvent {}
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

    public class StreamResponse<T>
    {
        public T Result { get; set; }
        public string Error { get; set; }
    }

    public partial class ArmadaClient
    {
        public async Task<IEnumerable<StreamResponse<ApiEventStreamMessage>>> GetJobEventsStream(string jobSetId,
            string fromMessage = null, bool watch = false)
        {
            var fileResponse = await GetJobSetEventsCoreAsync(jobSetId,
                new ApiJobSetRequest {FromMessageId = fromMessage, Watch = watch});
            return ReadEventStream(fileResponse.Stream);
        }

        private IEnumerable<StreamResponse<ApiEventStreamMessage>> ReadEventStream(Stream stream)
        {
            using (var reader = new StreamReader(stream))
            {
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var eventMessage =
                        JsonConvert.DeserializeObject<StreamResponse<ApiEventStreamMessage>>(line,
                            this.JsonSerializerSettings);
                    yield return eventMessage;
                }
            }
        }
    }
}