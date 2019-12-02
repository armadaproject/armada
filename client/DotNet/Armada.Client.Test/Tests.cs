using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using GResearch.Armada.Client;

namespace GResearch.Armada.Client.Test
{
    public class Tests
    {
        [Test]
        public async Task TestSimpleJobSubmitFlow()
        {
            var jobSet = $"set-{Guid.NewGuid()}";

            var client = new ArmadaClient("http://localhost:8080", new HttpClient());
            await client.CreateQueueAsync("test", new ApiQueue {PriorityFactor = 200});

            var pod = new V1PodSpec
            {
                Containers = new[]
                {
                    new V1Container
                    {
                        Name = "Container1",
                        Image = "index.docker.io/library/ubuntu:latest",
                        Args = new[] {"sleep", "10s"},
                        Resources = new V1ResourceRequirements
                        {
                            Requests = new Dictionary<string, string>
                            {
                                ["cpu"] = "120m",
                                ["memory"] = "512Mi"
                            },
                            Limits = new Dictionary<string, string>
                            {
                                ["cpu"] = "120m",
                                ["memory"] = "512Mi"
                            }
                        }
                    }
                }
            };

            var request = new ApiJobSubmitRequest
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

            var response = await client.SubmitJobsAsync(request);
            var cancelResponse =
                await client.CancelJobsAsync(new ApiJobCancelRequest {Queue = "test", JobSetId = jobSet});
            var events = await client.GetJobEventsStream(jobSet, watch: false);
            var allEvents = events.ToList();

            Assert.That(allEvents, Is.Not.Empty);
            Assert.That(allEvents[0].Result.Message.Submitted, Is.Not.Null);
        }
    }
}