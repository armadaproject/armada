package scheduling

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
)

func Test_minimumJobSize(t *testing.T) {
	request := v1.ResourceList{"cpu": resource.MustParse("2"), "memory": resource.MustParse("2Gi")}
	resourceRequirement := v1.ResourceRequirements{
		Limits:   request,
		Requests: request,
	}
	job := &api.Job{PodSpec: &v1.PodSpec{Containers: []v1.Container{{Resources: resourceRequirement}}}}

	assert.True(t, isLargeEnough(job, common.ComputeResources{"cpu": resource.MustParse("2")}))
	assert.True(t, isLargeEnough(job, common.ComputeResources{}))

	assert.False(t, isLargeEnough(job, common.ComputeResources{"cpu": resource.MustParse("5")}))
	assert.False(t, isLargeEnough(job, common.ComputeResources{"gpu": resource.MustParse("1")}))
}

func Test_distributeRemainder_highPriorityUserDoesNotBlockOthers(t *testing.T) {

	queue1 := &api.Queue{Name: "queue1", PriorityFactor: 1}
	queue2 := &api.Queue{Name: "queue2", PriorityFactor: 1}

	scarcity := map[string]float64{"cpu": 1, "gpu": 1}

	priorities := map[*api.Queue]QueuePriorityInfo{
		queue1: {
			Priority:     1000,
			CurrentUsage: common.ComputeResources{"cpu": resource.MustParse("100"), "memory": resource.MustParse("80Gi")}},
		queue2: {
			Priority:     0.5,
			CurrentUsage: common.ComputeResources{"cpu": resource.MustParse("0"), "memory": resource.MustParse("0")}},
	}
	requestSize := common.ComputeResources{"cpu": resource.MustParse("10"), "memory": resource.MustParse("1Gi")}

	schedulingInfo := map[*api.Queue]*QueueSchedulingInfo{
		queue1: {remainingSchedulingLimit: requestSize.AsFloat(), schedulingShare: requestSize.AsFloat(), adjustedShare: requestSize.AsFloat()},
		queue2: {remainingSchedulingLimit: requestSize.AsFloat(), schedulingShare: requestSize.AsFloat(), adjustedShare: requestSize.AsFloat()},
	}

	impossiblePodSpec := classicPodSpec.DeepCopy()
	impossiblePodSpec.NodeSelector = map[string]string{"impossible": "label"}

	jobQueue := &fakeJobQueue{
		jobsByQueue: map[string][]*api.Job{
			"queue1": {
				&api.Job{PodSpec: classicPodSpec},
				&api.Job{PodSpec: classicPodSpec},
				&api.Job{PodSpec: classicPodSpec},
				&api.Job{PodSpec: classicPodSpec},
				&api.Job{PodSpec: classicPodSpec},
			},
			"queue2": {
				&api.Job{PodSpec: impossiblePodSpec},
			},
		},
	}

	// the leasing logic stops scheduling 1s before the deadline
	ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))

	nodeResources := common.ComputeResources{"cpu": resource.MustParse("100"), "memory": resource.MustParse("100Gi")}
	nodes := []api.NodeInfo{{Name: "testNode", AllocatableResources: nodeResources, AvailableResources: nodeResources}}
	c := leaseContext{
		ctx: ctx,
		schedulingConfig: &configuration.SchedulingConfig{
			QueueLeaseBatchSize: 10,
		},
		onJobsLeased:  func(a []*api.Job) {},
		clusterId:     "c1",
		nodeResources: AggregateNodeTypeAllocations(nodes),

		resourceScarcity:    scarcity,
		priorities:          priorities,
		queueSchedulingInfo: SliceResourceWithLimits(scarcity, schedulingInfo, priorities, requestSize.AsFloat()),
		queue:               jobQueue,
		queueCache:          map[string][]*api.Job{},
	}

	jobs, e := c.distributeRemainder(NewSchedulingLimit(1000, 1024*1024*8, 1024*50))
	assert.Nil(t, e)
	assert.Equal(t, 5, len(jobs))
}

func Test_distributeRemainder_DoesNotExceedSchedulingLimits(t *testing.T) {

	queue1 := &api.Queue{Name: "queue1", PriorityFactor: 1}

	scarcity := map[string]float64{"cpu": 1, "gpu": 1}

	priorities := map[*api.Queue]QueuePriorityInfo{
		queue1: {
			Priority:     1000,
			CurrentUsage: common.ComputeResources{"cpu": resource.MustParse("100"), "memory": resource.MustParse("80Gi")}},
	}
	requestSize := common.ComputeResources{"cpu": resource.MustParse("10"), "memory": resource.MustParse("1Gi")}
	resourceLimit := common.ComputeResources{"cpu": resource.MustParse("2.5"), "memory": resource.MustParse("2.5Gi")}.AsFloat()

	schedulingInfo := map[*api.Queue]*QueueSchedulingInfo{
		queue1: {remainingSchedulingLimit: resourceLimit, schedulingShare: resourceLimit, adjustedShare: resourceLimit},
	}

	repository := &fakeJobQueue{
		jobsByQueue: map[string][]*api.Job{
			"queue1": {
				&api.Job{PodSpec: classicPodSpec},
				&api.Job{PodSpec: classicPodSpec},
				&api.Job{PodSpec: classicPodSpec},
				&api.Job{PodSpec: classicPodSpec},
				&api.Job{PodSpec: classicPodSpec},
			},
		},
	}

	// the leasing logic stops scheduling 1s before the deadline
	ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))

	nodeResources := common.ComputeResources{"cpu": resource.MustParse("100"), "memory": resource.MustParse("100Gi")}
	nodes := []api.NodeInfo{{Name: "testNode", AllocatableResources: nodeResources, AvailableResources: nodeResources}}

	c := leaseContext{
		ctx: ctx,
		schedulingConfig: &configuration.SchedulingConfig{
			QueueLeaseBatchSize: 10,
		},
		onJobsLeased: func(a []*api.Job) {},
		clusterId:    "c1",

		nodeResources: AggregateNodeTypeAllocations(nodes),

		resourceScarcity:    scarcity,
		priorities:          priorities,
		queueSchedulingInfo: SliceResourceWithLimits(scarcity, schedulingInfo, priorities, requestSize.AsFloat()),
		queue:               repository,
		queueCache:          map[string][]*api.Job{},
	}

	jobs, e := c.distributeRemainder(NewSchedulingLimit(1000, 1024*1024*8, 1024*50))
	assert.Nil(t, e)
	assert.Equal(t, 2, len(jobs))
}

func Test_calculateQueueSchedulingLimits(t *testing.T) {
	queue1 := &api.Queue{Name: "queue1", PriorityFactor: 1}
	activeQueues := []*api.Queue{queue1}
	schedulingLimitPerQueue := common.ComputeResourcesFloat{"cpu": 300.0}
	resourceLimitPerQueue := common.ComputeResourcesFloat{"cpu": 400.0}
	totalCapacity := &common.ComputeResources{"cpu": resource.MustParse("1000")}
	currentQueueResourceAllocation := map[string]common.ComputeResources{queue1.Name: {"cpu": resource.MustParse("250")}}

	result := calculateQueueSchedulingLimits(activeQueues, schedulingLimitPerQueue, resourceLimitPerQueue, totalCapacity, currentQueueResourceAllocation)

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[queue1].remainingSchedulingLimit, common.ComputeResourcesFloat{"cpu": 150.0})
}

func Test_calculateQueueSchedulingLimits_WithSmallSchedulingLimitPerQueue(t *testing.T) {
	queue1 := &api.Queue{Name: "queue1", PriorityFactor: 1}
	activeQueues := []*api.Queue{queue1}
	schedulingLimitPerQueue := common.ComputeResourcesFloat{"cpu": 100.0}
	resourceLimitPerQueue := common.ComputeResourcesFloat{"cpu": 400.0}
	totalCapacity := &common.ComputeResources{"cpu": resource.MustParse("1000")}
	currentQueueResourceAllocation := map[string]common.ComputeResources{queue1.Name: {"cpu": resource.MustParse("250")}}

	result := calculateQueueSchedulingLimits(activeQueues, schedulingLimitPerQueue, resourceLimitPerQueue, totalCapacity, currentQueueResourceAllocation)

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[queue1].remainingSchedulingLimit, common.ComputeResourcesFloat{"cpu": 100.0})
}

func Test_calculateQueueSchedulingLimits_WithCustomQueueLimitsLessThanGlobal(t *testing.T) {
	queue1 := &api.Queue{Name: "queue1", PriorityFactor: 1, ResourceLimits: map[string]float64{"cpu": 0.3}}
	activeQueues := []*api.Queue{queue1}
	schedulingLimitPerQueue := common.ComputeResourcesFloat{"cpu": 300.0}
	resourceLimitPerQueue := common.ComputeResourcesFloat{"cpu": 400.0}
	totalCapacity := &common.ComputeResources{"cpu": resource.MustParse("1000")}
	currentQueueResourceAllocation := map[string]common.ComputeResources{queue1.Name: {"cpu": resource.MustParse("250")}}

	result := calculateQueueSchedulingLimits(activeQueues, schedulingLimitPerQueue, resourceLimitPerQueue, totalCapacity, currentQueueResourceAllocation)

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[queue1].remainingSchedulingLimit, common.ComputeResourcesFloat{"cpu": 50.0})
}

func Test_calculateQueueSchedulingLimits_WithCustomQueueLimitsGreaterThanGlobal(t *testing.T) {
	queue1 := &api.Queue{Name: "queue1", PriorityFactor: 1, ResourceLimits: map[string]float64{"cpu": 0.5}}
	activeQueues := []*api.Queue{queue1}
	schedulingLimitPerQueue := common.ComputeResourcesFloat{"cpu": 300.0}
	resourceLimitPerQueue := common.ComputeResourcesFloat{"cpu": 400.0}
	totalCapacity := &common.ComputeResources{"cpu": resource.MustParse("1000")}
	currentQueueResourceAllocation := map[string]common.ComputeResources{queue1.Name: {"cpu": resource.MustParse("250")}}

	result := calculateQueueSchedulingLimits(activeQueues, schedulingLimitPerQueue, resourceLimitPerQueue, totalCapacity, currentQueueResourceAllocation)

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[queue1].remainingSchedulingLimit, common.ComputeResourcesFloat{"cpu": 250.0})
}

var classicPodSpec = &v1.PodSpec{
	Containers: []v1.Container{{
		Name:  "Container1",
		Image: "index.docker.io/library/ubuntu:latest",
		Args:  []string{"sleep", "10s"},
		Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Mi")},
			Limits:   v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Mi")},
		}}}}

type fakeJobQueue struct {
	jobsByQueue map[string][]*api.Job
}

func (r *fakeJobQueue) PeekClusterQueue(clusterId, queue string, limit int64) ([]*api.Job, error) {
	jobs, exists := r.jobsByQueue[queue]
	if !exists {
		return []*api.Job{}, nil
	}
	if int64(len(jobs)) > limit {
		jobs = jobs[:limit]
	}
	return jobs, nil
}

func (r *fakeJobQueue) TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error) {
	remainingJobs := []*api.Job{}
outer:
	for _, j := range r.jobsByQueue[queue] {
		for _, l := range jobs {
			if j == l {
				continue outer
			}
		}
		remainingJobs = append(remainingJobs, j)
	}
	r.jobsByQueue[queue] = remainingJobs
	return jobs, nil
}
