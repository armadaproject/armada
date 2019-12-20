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

namespace GResearch.Armada.Client.Test
{
    public class Tests
    {
        [Test]
        [Explicit("Intended for manual testing against armada server with proxy.")]
        public async Task TestWatchingEvents()
        {
            var client = new ArmadaClient("http://localhost:8080", new HttpClient());
            
            var jobSet = $"set-{Guid.NewGuid()}";
            
            // produce some events
            await client.CreateQueueAsync("test", new ApiQueue {PriorityFactor = 200});
            var request = CreateJobRequest(jobSet);
            var response = await client.SubmitJobsAsync(request);
            var cancelResponse = await client.CancelJobsAsync(new ApiJobCancelRequest {Queue = "test", JobSetId = jobSet});
            
            using (var cts = new CancellationTokenSource())
            {
                var eventCount = 0;
                Task.Run(() => client.WatchEvents(jobSet, null,  cts.Token, m => eventCount++, e => throw e));
                await Task.Delay(TimeSpan.FromMinutes(2));
                cts.Cancel();
                Assert.That(eventCount, Is.GreaterThan(0));
            }
        }
        
        [Test]
        public async Task TestSimpleJobSubmitFlow()
        {
            var jobSet = $"set-{Guid.NewGuid()}";

            IArmadaClient client = new ArmadaClient("http://localhost:8080", new HttpClient());
            await client.CreateQueueAsync("test", new ApiQueue {PriorityFactor = 200});

            var request = CreateJobRequest(jobSet);

            var response = await client.SubmitJobsAsync(request);
            var cancelResponse =
                await client.CancelJobsAsync(new ApiJobCancelRequest {Queue = "test", JobSetId = jobSet});
            var events = await client.GetJobEventsStream(jobSet, watch: false);
            var allEvents = events.ToList();

            Assert.That(allEvents, Is.Not.Empty);
            Assert.That(allEvents[0].Result.Message.Submitted, Is.Not.Null);
        }

        private static ApiJobSubmitRequest CreateJobRequest(string jobSet)
        {
            var pod = new V1PodSpec
            {
                Volumes = new List<V1Volume>
                {
                    new V1Volume
                    {
                        Name = "root-dir",
                        FlexVolume = new V1FlexVolumeSource
                        {
                            Driver = "gr/cifs",
                            FsType = "cifs",
                            SecretRef = new V1LocalObjectReference {Name = "secret-name"},
                            Options = new Dictionary<string, string> {{"networkPath", ""}}
                        }
                    }
                },
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