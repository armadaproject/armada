package armada

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/configuration"
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/alicebob/miniredis"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"log"
	"testing"
)

func TestSubmitJob(t *testing.T) {
	withRunningServer(func(client api.SubmitClient, leaseClient api.AggregatedQueueClient, ctx context.Context) {

		_, err := client.CreateQueue(ctx, &api.Queue{
			Name:           "test",
			PriorityFactor: 1,
		})
		assert.Empty(t, err)

		cpu, _ := resource.ParseQuantity("1")
		memory, _ := resource.ParseQuantity("512Mi")

		response, err := client.SubmitJob(ctx, &api.JobRequest{
			PodSpec: &v1.PodSpec{
				Containers: []v1.Container{{
					Name:  "Container1",
					Image: "index.docker.io/library/ubuntu:latest",
					Args:  []string{"sleep", "10s"},
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{"cpu": cpu, "memory": memory},
					},
				},
				},
			},
			Priority: 0,
			Queue:    "test",
		})
		assert.Empty(t, err)

		leasedResponse, err := leaseClient.LeaseJobs(ctx, &api.LeaseRequest{
			ClusterID: "test-cluster",
			Resources: common.ComputeResources{"cpu": cpu, "memory": memory},
		})
		assert.Empty(t, err)

		assert.Equal(t, 1, len(leasedResponse.Job))
		assert.Equal(t, response.JobId, leasedResponse.Job[0].Id)
	})
}

func withRunningServer(action func(client api.SubmitClient, leaseClient api.AggregatedQueueClient, ctx context.Context)) {
	redis, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer redis.Close()

	server, _ := Serve(&configuration.ArmadaConfig{
		GrpcPort: ":50051",
		Redis: configuration.RedisConfig{
			Addr: redis.Addr(),
			Db:   0,
		},
	})
	defer server.Stop()

	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := api.NewSubmitClient(conn)
	leaseClient := api.NewAggregatedQueueClient(conn)
	ctx := context.Background()

	action(client, leaseClient, ctx)
}
