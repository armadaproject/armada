package armada

import (
	"context"
	"fmt"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/configuration"
	"github.com/alicebob/miniredis"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"log"
	"testing"
	"time"
)

func TestSubmitJob(t *testing.T) {
	withRunningServer(func(client api.SubmitClient, ctx context.Context) {
		response, err := client.SubmitJob(ctx, &api.JobRequest{
			PodSpec: &v1.PodSpec{
				Containers: []v1.Container {
					{
						Name:  "Container1",
						Image: "index.docker.io/library/ubuntu:latest",
						Args:  []string { "sleep", "10s"},
					},
				},
			},
			Priority: 0,
			Queue: "test",
		})
		assert.Empty(t, err)
		fmt.Println(response)
	})
}

func withRunningServer(action func (client api.SubmitClient, ctx context.Context)) {
	redis, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer redis.Close()

	server, _ := Serve(&configuration.ArmadaConfig{
		GrpcPort:":50051",
		Redis: configuration.RedisConfig{
			Addr: redis.Addr(),
			Db:   0,
		},
	})
	defer server.Stop()

	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := api.NewSubmitClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	action(client, ctx)
}
