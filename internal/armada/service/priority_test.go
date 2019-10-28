package service

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/armada/types"
)

func TestPriorityService_GetQueuePriorities(t *testing.T) {

	q1 := &api.Queue{Name: "queue1", PriorityFactor: 2}
	q2 := &api.Queue{Name: "queue2", PriorityFactor: 1}
	q3 := &api.Queue{Name: "queue3", PriorityFactor: 1}
	q4 := &api.Queue{Name: "queue4", PriorityFactor: 1}
	q5 := &api.Queue{Name: "queue5", PriorityFactor: 0.5}

	cpu := resource.MustParse("1Mi")
	queue1usage := &api.QueueReport{
		Name:      "queue1",
		Resources: map[string]resource.Quantity{"cpu": cpu},
	}

	service := NewMultiClusterPriorityService(
		mockUsageRepository{
			clusterUsageReports: map[string]*api.ClusterUsageReport{
				"cluster1": {ClusterId: "cluster1", ReportTime: time.Now(), Queues: []*api.QueueReport{queue1usage}},
				"cluster2": {ClusterId: "cluster2", ReportTime: time.Now(), Queues: []*api.QueueReport{queue1usage}},
			},
			clusterPriorities: map[string]map[string]float64{
				"cluster1": {
					"queue1": 1.5,
					"queue2": 1.5,
				},
				"cluster2": {
					"queue1": 1,
					"queue3": 1,
					"queue6": 1, // missing
				},
			},
		},
		mockQueueRepository{
			queues: []*api.Queue{q1, q2, q3, q4, q5},
		},
		mockMetricRecorder{})

	priorities, e := service.GetQueuePriorities()
	assert.Nil(t, e)

	cpuSum := cpu.DeepCopy()
	cpuSum.Add(cpu)
	assert.Equal(t, map[*api.Queue]types.QueuePriorityInfo{
		q1: {5, map[string]resource.Quantity{"cpu": cpuSum}},
		q2: {1.5, nil},
		q3: {1, nil},
		q4: {minPriority, nil},
		q5: {minPriority * 0.5, nil},
	}, priorities)
}

type mockQueueRepository struct {
	queues []*api.Queue
}

func (m mockQueueRepository) GetQueue(name string) (*api.Queue, error) {
	for _, q := range m.queues {
		if q.Name == name {
			return q, nil
		}
	}
	return nil, errors.New("not found")
}

func (m mockQueueRepository) GetAllQueues() ([]*api.Queue, error) {
	return m.queues, nil
}

func (mockQueueRepository) CreateQueue(queue *api.Queue) error {
	panic("implement me")
}

type mockUsageRepository struct {
	clusterPriorities   map[string]map[string]float64
	clusterUsageReports map[string]*api.ClusterUsageReport
}

func (m mockUsageRepository) GetClusterUsageReports() (map[string]*api.ClusterUsageReport, error) {
	return m.clusterUsageReports, nil
}

func (mockUsageRepository) GetClusterPriority(clusterId string) (map[string]float64, error) {
	panic("implement me")
}

func (m mockUsageRepository) GetClusterPriorities(clusterIds []string) (map[string]map[string]float64, error) {
	return m.clusterPriorities, nil
}

func (m mockUsageRepository) UpdateCluster(report *api.ClusterUsageReport, priorities map[string]float64) error {
	panic("implement me")
}

type mockMetricRecorder struct{}

func (mockMetricRecorder) RecordQueuePriorities(priorities map[*api.Queue]types.QueuePriorityInfo) {}
