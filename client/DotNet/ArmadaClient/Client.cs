using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace ArmadaClient
{
    public class StreamResponse<T>
    {
        public T Result { get; set; }
        public string Error { get; set; }
    }

    public partial class Client
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