using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Linq;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using GResearch.Armada.Client;
using RichardSzalay.MockHttp;

namespace GResearch.Armada.Client.Test
{
    public class Tests
    {
        [Test]
        [Explicit("Intended for manual testing against armada server with proxy.")]
        public async Task TestWatchingEvents()
        {
            var client = new ArmadaClient("http://localhost:8080", new HttpClient());

            var queue = "test";
            var jobSet = $"set-{Guid.NewGuid()}";

            // produce some events
            await client.CreateQueueAsync(new ApiQueue {Name = queue, PriorityFactor = 200});
            var request = CreateJobRequest(jobSet);
            var response = await client.SubmitJobsAsync(request);
            var cancelResponse =
                await client.CancelJobsAsync(new ApiJobCancelRequest {Queue = "test", JobSetId = jobSet});

            using (var cts = new CancellationTokenSource())
            {
                var eventCount = 0;
                Task.Run(() => client.WatchEvents(queue, jobSet, null, cts.Token, m => eventCount++, e => throw e));
                await Task.Delay(TimeSpan.FromMinutes(2));
                cts.Cancel();
                Assert.That(eventCount, Is.EqualTo(4));
            }
        }

        [Test]
        public async Task TestSimpleJobSubmitFlow()
        {
            var queue = "test";
            var jobSet = $"set-{Guid.NewGuid()}";

            IArmadaClient client = new ArmadaClient("http://localhost:8080", new HttpClient());
            await client.CreateQueueAsync(new ApiQueue {Name = queue, PriorityFactor = 200});

            var request = CreateJobRequest(jobSet);

            var response = await client.SubmitJobsAsync(request);
            var cancelResponse =
                await client.CancelJobsAsync(new ApiJobCancelRequest {Queue = "test", JobSetId = jobSet});
            var events = await client.GetJobEventsStream(queue, jobSet, watch: false);
            var allEvents = events.ToList();

            Assert.That(allEvents, Is.Not.Empty);
            Assert.That(allEvents[0].Result.Message.Submitted, Is.Not.Null);
        }

        [Test]
        public async Task TestProcessingUnknownEvents()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When("http://localhost:8080/*")
                .Respond("application/json",
                    @"{""result"":{""Id"":""1593611590122-0"",""message"":{""Queued"":{""JobId"":""01ec5ae6f9wvya6cr6stzwty7v"",""JobSetId"":""set-bae48cc8-9f70-465f-ae5c-c92713b5f24f"",""Queue"":""test"",""Created"":""2020-07-01T13:53:10.122263955Z""}}}}
                    {""result"":{""Id"":""1593611590122-0"",""message"":{""UnknownEvent"":""test""}}}
                    {""error"": ""test error""}
                    {}
                    
                    {""a"":""b""}");

            IArmadaClient client = new ArmadaClient("http://localhost:8080", new HttpClient(mockHttp));
            var events = (await client.GetJobEventsStream("queue", "jobSet", watch: false)).ToList();
            Assert.That(events.Count(), Is.EqualTo(2));
            Assert.That(events[0].Result.Message.Event, Is.Not.Null);
            Assert.That(events[1].Error, Is.EqualTo("test error"));
        }

        private static ApiJobSubmitRequest CreateJobRequest(string jobSet)
        {
            var pod = new V1PodSpec
            {
                Containers = new[]
                {
                    new V1Container
                    {
                        Name = "Container1",
                        Image = "index.docker.io/library/ubuntu:latest",
                        Args = new[] {"sleep", "10s"},
                        SecurityContext = new V1SecurityContext {RunAsUser = 1000},
                        Resources = new V1ResourceRequirements
                        {
                            Requests = new V1ResourceList
                            {
                                ["cpu"] = "120m",
                                ["memory"] = "512Mi"
                            },
                            Limits = new V1ResourceList
                            {
                                ["cpu"] = "120m",
                                ["memory"] = "512Mi"
                            }
                        }
                    }
                }
            };

            return new ApiJobSubmitRequest
            {
                Queue = "test",
                JobSetId = jobSet,
                JobRequestItems = new[]
                {
                    new ApiJobSubmitRequestItem
                    {
                        Priority = 1,
                        PodSpec = pod
                    }
                },
            };
        }
    }
}
