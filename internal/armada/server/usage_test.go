package server

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

func TestUsageServer_ReportUsage(t *testing.T) {
	withUsageServer(&configuration.SchedulingConfig{}, func(s *UsageServer) {
		now := time.Now()
		cpu, _ := resource.ParseQuantity("10")
		memory, _ := resource.ParseQuantity("360Gi")

		err := s.queueRepository.CreateQueue(queue.Queue{Name: "q1", PriorityFactor: 1})
		assert.Nil(t, err)

		_, err = s.ReportUsage(context.Background(), oneQueueReport(now, cpu, memory))
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

func TestUsageServer_ReportUsageWithDefinedScarcity(t *testing.T) {
	withUsageServer(&configuration.SchedulingConfig{ResourceScarcity: map[string]float64{"cpu": 1.0}}, func(s *UsageServer) {
		now := time.Now()
		cpu, _ := resource.ParseQuantity("10")
		memory, _ := resource.ParseQuantity("360Gi")

		err := s.queueRepository.CreateQueue(queue.Queue{Name: "q1", PriorityFactor: 1})
		assert.Nil(t, err)

		_, err = s.ReportUsage(context.Background(), oneQueueReport(now, cpu, memory))
		assert.Nil(t, err)

		priority, err := s.usageRepository.GetClusterPriority("clusterA")
		assert.Nil(t, err)
		assert.Equal(t, 5.0, priority["q1"], "Priority should be updated for the new cluster.")

		_, err = s.ReportUsage(context.Background(), oneQueueReport(now.Add(time.Minute), cpu, memory))
		assert.Nil(t, err)

		priority, err = s.usageRepository.GetClusterPriority("clusterA")
		assert.Nil(t, err)
		assert.Equal(t, 7.5, priority["q1"], "Priority should be updated considering previous report.")
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

func withUsageServer(schedulingConfig *configuration.SchedulingConfig, action func(s *UsageServer)) {
	db, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: db.Addr()})

	repo := repository.NewRedisUsageRepository(redisClient)
	queueRepo := repository.NewRedisQueueRepository(redisClient)
	server := NewUsageServer(&FakePermissionChecker{}, time.Minute, schedulingConfig, repo, queueRepo)

	action(server)
}
