using ApiEvent = ArmadaProject.Io.Api.Event;
using ArmadaProject.Io.Api;
using Grpc.Core;
using Grpc.Net.Client;
using K8S.Io.Api.Core.V1;
using K8S.Io.Apimachinery.Pkg.Api.Resource;
using NUnit.Framework;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ArmadaProject.Io.Client.Test
{
    public class Tests
    {
        [Test]
        public async Task TestSimpleJobSubmitFlow()
        {
            var queue = "test";
            var jobSetId = Guid.NewGuid().ToString();
            var channel = CreateChannel(new Uri("http://localhost:8080"), ChannelCredentials.Insecure);

            try
            {
                //submit
                var submitClient = new Submit.SubmitClient(channel);
                var submitRequest = CreateSubmitRequest(queue, jobSetId);
                var submitResponse = await submitClient.SubmitJobsAsync(submitRequest);

                //watch events
                var eventClient = new ApiEvent.EventClient(channel);
                var watchRequest = new JobSetRequest
                {
                    Queue = queue,
                    Id = jobSetId,
                    Watch = true,
                };
                var watchResponse = eventClient.GetJobSetEvents(watchRequest);
                var submittedEventReceived = false;
                using (var cts = new CancellationTokenSource())
                {
                    cts.CancelAfter(TimeSpan.FromSeconds(30));

                    while (await watchResponse.ResponseStream.MoveNext(cts.Token))
                    {
                        if (watchResponse.ResponseStream.Current.Message.EventsCase == EventMessage.EventsOneofCase.Submitted)
                        {
                            submittedEventReceived = true;
                            break;
                        }
                    }
                }

                Assert.That(submittedEventReceived, Is.Not.False);
            }
            finally
            {
                await channel.ShutdownAsync();
            }
        }

        private static GrpcChannel CreateChannel(Uri address, ChannelCredentials credentials)
        {
            var options = new GrpcChannelOptions
            {
                Credentials = credentials,
            };

            return GrpcChannel.ForAddress(address, options);
        }

        private static JobSubmitRequest CreateSubmitRequest(string queue, string jobSetId)
        {
            var resourceRequirements = new ResourceRequirements();
            resourceRequirements.Requests["cpu"] = new Quantity { String = "120m" };
            resourceRequirements.Requests["memory"] = new Quantity { String = "512Mi" };
            resourceRequirements.Limits["cpu"] = new Quantity { String = "120m" };
            resourceRequirements.Limits["memory"] = new Quantity { String = "512Mi" };

            var container = new Container();
            container.Name = "Container1";
            container.Image = "index.docker.io/library/ubuntu:latest";
            container.Args.AddRange(new string[] { "sleep", "10s" });
            container.SecurityContext = new SecurityContext { RunAsUser = 1000 };
            container.Resources = resourceRequirements;

            var podSpec = new PodSpec();
            podSpec.Containers.Add(container);

            var requestItem = new JobSubmitRequestItem();
            requestItem.Priority = 1;
            requestItem.PodSpecs.Add(podSpec);

            var request = new JobSubmitRequest();
            request.Queue = queue;
            request.JobSetId = jobSetId;
            request.JobRequestItems.Add(requestItem);

            return request;
        }
    }
}
