using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using RichardSzalay.MockHttp;

namespace GResearch.Armada.Client.Test
{
    public class Tests
    {
        public static DateTimeOffset now = (DateTimeOffset)DateTime.UtcNow;

        // ARMADA_SERVER should be defined in this process's environment, of the
        // format  https://armada-server-hostname/api
        public static string testServer = Environment.GetEnvironmentVariable("ARMADA_SERVER");

        [Test]
        public async Task TestWatchingEvents()
        {
            var testQueue = "test-" + Guid.NewGuid();

            Assert.That(testServer, Is.Not.Null);
            var client = new ArmadaClient(testServer, new HttpClient());

            var jobSet = $"set-{Guid.NewGuid()}";

            Console.WriteLine("---- creating queue  " + testQueue);

            // produce some events
            await client.CreateQueueAsync(new ApiQueue {Name = testQueue, PriorityFactor = 200});

            var request = CreateJobRequest(jobSet, testQueue);
            var response = await client.SubmitJobsAsync(request);
            Assert.That(response.JobResponseItems.Count, Is.EqualTo(1));

            foreach (var jobRespItem in response.JobResponseItems) {
                Assert.That(jobRespItem.Error, Is.Null);
            }

            var cancelResponse =
                await client.CancelJobsAsync(new ApiJobCancelRequest {Queue = testQueue, JobSetId = jobSet});

            using (var cts = new CancellationTokenSource())
            {
                var eventCount = 0;
                Task.Run(() => client.WatchEvents(testQueue, jobSet, null, cts.Token, m => eventCount++, e => throw e));
                await Task.Delay(TimeSpan.FromMinutes(2));
                cts.Cancel();
                Assert.That(eventCount, Is.EqualTo(4));
            }

            client.DeleteQueueAsync(testQueue);
        }

        [Test]
        public async Task TestSimpleJobSubmitFlow()
        {
            var testQueue = "test-" + Guid.NewGuid();
            var jobSet = $"set-{Guid.NewGuid()}";

            Assert.That(testServer, Is.Not.Null);
            IArmadaClient client = new ArmadaClient(testServer, new HttpClient());
            await client.CreateQueueAsync(new ApiQueue {Name = testQueue, PriorityFactor = 200});

            var request = CreateJobRequest(jobSet, testQueue);

            var response = await client.SubmitJobsAsync(request);
            var cancelResponse =
                await client.CancelJobsAsync(new ApiJobCancelRequest {Queue = testQueue, JobSetId = jobSet});
            var events = await client.GetJobEventsStream(testQueue, jobSet, watch: false);

            var eventsEnum = events.GetEnumerator();
            while (eventsEnum.MoveNext()) {
                var ev = eventsEnum.Current;
                 Assert.That(ev, Is.Not.Null);
                Assert.That(ev.Result.Message.Submitted, Is.Not.Null);
            }
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
            var events = (await client.GetJobEventsStream("queue", "jobSet", watch: false));

            var eventsEnum = events.GetEnumerator();
            var n = 0;

            while (eventsEnum.MoveNext()) {
                var ev = eventsEnum.Current;
                if (n == 0) {
                    Assert.That(ev.Result.Message.Event, Is.Not.Null);
                } else if (n == 1) {
                    Assert.That(ev.Error, Is.EqualTo("test error"));
                }
                n++;
            }
            Assert.That(n, Is.EqualTo(2));
        }

        private static ApiJobSubmitRequest CreateJobRequest(string jobSet, string testQueue)
        {
            var pod = new V1PodSpec
            {
               Containers = new[]
                {
                    new V1Container
                    {
                        Name = "container-1",
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
                Queue = testQueue,
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
