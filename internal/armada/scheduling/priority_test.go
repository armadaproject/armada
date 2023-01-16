package scheduling

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/pkg/api"
)

func TestPriorityService_GetQueuePriorities(t *testing.T) {
	q1 := &api.Queue{Name: "queue1", PriorityFactor: 2}
	q2 := &api.Queue{Name: "queue2", PriorityFactor: 1}
	q3 := &api.Queue{Name: "queue3", PriorityFactor: 1}
	q4 := &api.Queue{Name: "queue4", PriorityFactor: 1}
	q5 := &api.Queue{Name: "queue5", PriorityFactor: 1.5}

	cpu := resource.MustParse("1Mi")
	queue1usage := &api.QueueReport{
		Name:      "queue1",
		Resources: map[string]resource.Quantity{"cpu": cpu},
	}

	clusterUsageReports := map[string]*api.ClusterUsageReport{
		"cluster1": {ClusterId: "cluster1", ReportTime: time.Now(), Queues: []*api.QueueReport{queue1usage}},
		"cluster2": {ClusterId: "cluster2", ReportTime: time.Now(), Queues: []*api.QueueReport{queue1usage}},
	}
	clusterPriorities := map[string]map[string]float64{
		"cluster1": {
			"queue1": 1.5,
			"queue2": 1.5,
		},
		"cluster2": {
			"queue1": 1,
			"queue3": 1,
			"queue6": 1, // missing
		},
	}
	queues := []*api.Queue{q1, q2, q3, q4, q5}

	priorities := CalculateQueuesPriorityInfo(clusterPriorities, clusterUsageReports, queues)

	cpuSum := cpu.DeepCopy()
	cpuSum.Add(cpu)
	assert.Equal(t, map[*api.Queue]QueuePriorityInfo{
		q1: {5, map[string]resource.Quantity{"cpu": cpuSum}},
		q2: {1.5, nil},
		q3: {1, nil},
		q4: {minPriority, nil},
		q5: {minPriority, nil},
	}, priorities)
}

func TestAggregateQueueUsageDoesNotChangeSourceData(t *testing.T) {
	oneCpu := resource.MustParse("1")
	reports := map[string]*api.ClusterUsageReport{
		"cluster1": {Queues: []*api.QueueReport{
			{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
		}},
		"cluster2": {Queues: []*api.QueueReport{
			{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
		}},
	}
	aggregateQueueUsage(reports)

	for _, r := range reports {
		assert.Equal(t, r.Queues[0].Resources["cpu"], oneCpu)
	}
}
