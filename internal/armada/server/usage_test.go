package server

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
	"time"
)

func TestUsageServer_ReportUsage(t *testing.T) {
	withUsageServer(func(s *UsageServer) {
		now := time.Now()
		cpu, _ := resource.ParseQuantity("10")
		memory, _ := resource.ParseQuantity("360Gi")

		_, err := s.ReportUsage(context.Background(), oneQueueReport(now, cpu, memory))
		assert.Nil(t, err)

		priority, err := s.usageRepository.GetClusterPriority("clusterA")
		assert.Nil(t, err)
		assert.Equal(t, 10.0, priority["q1"], "Priority should be updated for the new cluster.")

		_, err = s.ReportUsage(context.Background(), oneQueueReport(now.Add(time.Minute), cpu, memory))
		assert.Nil(t, err)

		priority, err = s.usageRepository.GetClusterPriority("clusterA")
		assert.Nil(t, err)
		assert.Equal(t, 15.0, priority["q1"], "Priority should be updated considering previous report.")
	})
}

func oneQueueReport(t time.Time, cpu resource.Quantity, memory resource.Quantity) *api.ClusterUsageReport {
	return &api.ClusterUsageReport{
		ClusterId:       "clusterA",
		ReportTime:      t,
		ClusterCapacity: common.ComputeResources{"cpu": cpu, "memory": memory},
		Queues: []*api.QueueReport{
			{
				Name:      "q1",
				Resources: common.ComputeResources{"cpu": cpu, "memory": memory},
			},
		},
	}
}

func withUsageServer(action func(s *UsageServer)) {
	db, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	repo := repository.NewRedisUsageRepository(redis.NewClient(&redis.Options{Addr: db.Addr()}))
	server := NewUsageServer(time.Minute, repo)

	action(server)
}
