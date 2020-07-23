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

func Test_matchNodeLabels(t *testing.T) {
	job := &api.Job{PodSpec: &v1.PodSpec{NodeSelector: map[string]string{"armada/region": "eu", "armada/zone": "1"}}}

	assert.False(t, matchNodeLabels(job, &api.ClusterSchedulingInfoReport{}))
	assert.False(t, matchNodeLabels(job, &api.ClusterSchedulingInfoReport{AvailableLabels: []*api.NodeLabeling{
		{Labels: map[string]string{"armada/region": "eu"}},
		{Labels: map[string]string{"armada/zone": "2"}},
	}}))
	assert.False(t, matchNodeLabels(job, &api.ClusterSchedulingInfoReport{AvailableLabels: []*api.NodeLabeling{
		{Labels: map[string]string{"armada/region": "eu", "armada/zone": "2"}},
	}}))

	assert.True(t, matchNodeLabels(job, &api.ClusterSchedulingInfoReport{AvailableLabels: []*api.NodeLabeling{
		{Labels: map[string]string{"x": "y"}},
		{Labels: map[string]string{"armada/region": "eu", "armada/zone": "1", "x": "y"}},
	}}))
}

func Test_isAbleToFitOnAvailableNodes(t *testing.T) {
	request := v1.ResourceList{"cpu": resource.MustParse("2"), "memory": resource.MustParse("2Gi")}
	resourceRequirement := v1.ResourceRequirements{
		Limits:   request,
		Requests: request,
	}
	job := &api.Job{PodSpec: &v1.PodSpec{Containers: []v1.Container{{Resources: resourceRequirement}}}}

	assert.False(t, isAbleToFitOnAvailableNodes(job, &api.ClusterSchedulingInfoReport{}))

	assert.False(t, isAbleToFitOnAvailableNodes(job, &api.ClusterSchedulingInfoReport{
		NodeSizes: []api.ComputeResource{{Resources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}}},
	}))

	assert.True(t, isAbleToFitOnAvailableNodes(job, &api.ClusterSchedulingInfoReport{
		NodeSizes: []api.ComputeResource{
			{Resources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}},
			{Resources: common.ComputeResources{"cpu": resource.MustParse("3"), "memory": resource.MustParse("3Gi")}},
		},
	}))
}

func Test_minimumJobSize(t *testing.T) {
	request := v1.ResourceList{"cpu": resource.MustParse("2"), "memory": resource.MustParse("2Gi")}
	resourceRequirement := v1.ResourceRequirements{
		Limits:   request,
		Requests: request,
	}
	job := &api.Job{PodSpec: &v1.PodSpec{Containers: []v1.Container{{Resources: resourceRequirement}}}}

	assert.True(t, isLargeEnough(job, &api.ClusterSchedulingInfoReport{
		MinimumJobSize: common.ComputeResources{"cpu": resource.MustParse("2")}}))
	assert.True(t, isLargeEnough(job, &api.ClusterSchedulingInfoReport{
		MinimumJobSize: common.ComputeResources{}}))

	assert.False(t, isLargeEnough(job, &api.ClusterSchedulingInfoReport{
		MinimumJobSize: common.ComputeResources{"cpu": resource.MustParse("5")}}))
	assert.False(t, isLargeEnough(job, &api.ClusterSchedulingInfoReport{
		MinimumJobSize: common.ComputeResources{"gpu": resource.MustParse("1")}}))
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

	repository := &fakeJobQueueRepository{
		jobsByQueue: map[string][]*api.Job{
			"queue1": {
				&api.Job{PodSpec: classicPodSpec},
				&api.Job{PodSpec: classicPodSpec},
				&api.Job{PodSpec: classicPodSpec},
				&api.Job{PodSpec: classicPodSpec},
				&api.Job{PodSpec: classicPodSpec},
			},
			"queue2": {
				&api.Job{RequiredNodeLabels: map[string]string{"impossible": "label"}, PodSpec: classicPodSpec},
			},
		},
	}

	// the leasing logic stops scheduling 1s before the deadline
	ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))

	nodeResources := common.ComputeResources{"cpu": resource.MustParse("100"), "memory": resource.MustParse("100Gi")}
	leaseRequest := &api.LeaseRequest{
		ClusterId: "c1",
		Resources: requestSize,
		Nodes:     []api.NodeInfo{{Name: "testNode", AllocatableResources: nodeResources, AvailableResources: nodeResources}},
	}

	c := leaseContext{
		ctx: ctx,
		schedulingConfig: &configuration.SchedulingConfig{
			QueueLeaseBatchSize: 10,
		},
		onJobsLeased: func(a []*api.Job) {},
		request:      leaseRequest,

		clusterSchedulingInfo: CreateClusterSchedulingInfoReport(leaseRequest),
		resourceScarcity:      scarcity,
		priorities:            priorities,
		queueSchedulingInfo:   SliceResourceWithLimits(scarcity, schedulingInfo, priorities, requestSize.AsFloat()),
		repository:            repository,
		queueCache:            map[string][]*api.Job{},
	}

	jobs, e := c.distributeRemainder(1000)
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

	repository := &fakeJobQueueRepository{
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
	leaseRequest := &api.LeaseRequest{
		ClusterId: "c1",
		Resources: requestSize,
		Nodes:     []api.NodeInfo{{Name: "testNode", AllocatableResources: nodeResources, AvailableResources: nodeResources}},
	}
	c := leaseContext{
		ctx: ctx,
		schedulingConfig: &configuration.SchedulingConfig{
			QueueLeaseBatchSize: 10,
		},
		onJobsLeased: func(a []*api.Job) {},
		request:      leaseRequest,

		clusterSchedulingInfo: CreateClusterSchedulingInfoReport(leaseRequest),
		resourceScarcity:      scarcity,
		priorities:            priorities,
		queueSchedulingInfo:   SliceResourceWithLimits(scarcity, schedulingInfo, priorities, requestSize.AsFloat()),
		repository:            repository,
		queueCache:            map[string][]*api.Job{},
	}

	jobs, e := c.distributeRemainder(1000)
	assert.Nil(t, e)
	assert.Equal(t, 2, len(jobs))
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

type fakeJobQueueRepository struct {
	jobsByQueue map[string][]*api.Job
}

func (r *fakeJobQueueRepository) PeekQueue(queue string, limit int64) ([]*api.Job, error) {
	jobs, exists := r.jobsByQueue[queue]
	if !exists {
		return []*api.Job{}, nil
	}
	if int64(len(jobs)) > limit {
		jobs = jobs[:limit]
	}
	return jobs, nil
}

func (r *fakeJobQueueRepository) TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error) {
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

func Test_getDistinctNodesLabels(t *testing.T) {

	nodes := []api.NodeInfo{
		{Labels: map[string]string{
			"A": "x",
			"B": "x",
		}},
		{Labels: map[string]string{
			"A": "x",
			"B": "x",
		}},
		{Labels: map[string]string{
			"B": "y",
		}},
	}

	result := getDistinctNodesLabels(nodes)

	assert.Equal(t,
		[]*api.NodeLabeling{
			{Labels: map[string]string{"A": "x", "B": "x"}},
			{Labels: map[string]string{"B": "y"}},
		}, result)
}

func Test_getLargestNodeSizes(t *testing.T) {
	nodes := []api.NodeInfo{
		{Name: "node-1", AllocatableResources: makeResourceList(2, 1)},
		{Name: "node-2", AllocatableResources: makeResourceList(1, 2)},
	}

	largestNodeSizes := getLargestNodeSizes(nodes)
	assert.Equal(t, len(largestNodeSizes), 2)

	node3 := api.NodeInfo{Name: "node-3", AllocatableResources: makeResourceList(3, 3)}
	nodes = append(nodes, node3)
	largestNodeSizes = getLargestNodeSizes(nodes)
	assert.Equal(t, len(largestNodeSizes), 1)
	assert.Equal(t, largestNodeSizes[0].Resources, node3.AllocatableResources)
}

func Test_getLargestNodeSizes_ReducedByResourceUsedByNonManagedPods(t *testing.T) {
	nodes := []api.NodeInfo{
		{Name: "node-1", AllocatableResources: makeResourceList(10, 10)},
		{Name: "node-2", AllocatableResources: makeResourceList(5, 5)},
	}

	largestNodeSizes := getLargestNodeSizes(nodes)
	assert.Equal(t, 1, len(largestNodeSizes))
	assert.Equal(t, makeResourceList(10, 10), common.ComputeResources(largestNodeSizes[0].Resources))
}

func Test_fits(t *testing.T) {
	n := &api.NodeInfo{
		AllocatableResources: makeResourceList(4, 40),
		AvailableResources:   makeResourceList(1, 10),
	}

	assert.False(t, fits(makeResourceList(2, 20), n))
	assert.False(t, fits(makeResourceList(2, 5), n))
	assert.True(t, fits(makeResourceList(1, 10), n))
}

func Test_matchNodeSelector(t *testing.T) {
	n := &api.NodeInfo{
		Labels: map[string]string{
			"A": "test",
			"B": "test",
		},
	}
	assert.False(t, matchNodeSelector(&api.Job{PodSpec: &v1.PodSpec{NodeSelector: map[string]string{"C": "test"}}}, n))
	assert.False(t, matchNodeSelector(&api.Job{PodSpec: &v1.PodSpec{NodeSelector: map[string]string{"B": "42"}}}, n))
	assert.True(t, matchNodeSelector(&api.Job{PodSpec: &v1.PodSpec{NodeSelector: map[string]string{"A": "test"}}}, n))
	assert.True(t, matchNodeSelector(&api.Job{PodSpec: &v1.PodSpec{NodeSelector: map[string]string{"A": "test", "B": "test"}}}, n))
}

func Test_tolerates(t *testing.T) {
	n := &api.NodeInfo{
		Taints: []v1.Taint{
			{
				Key:    "A",
				Value:  "test",
				Effect: v1.TaintEffectNoSchedule,
			},
			{
				Key:    "B",
				Value:  "test",
				Effect: v1.TaintEffectPreferNoSchedule,
			},
		},
	}

	job1 := &api.Job{PodSpec: &v1.PodSpec{
		Tolerations: []v1.Toleration{
			{
				Key:      "A",
				Operator: v1.TolerationOpEqual,
				Value:    "test",
				Effect:   v1.TaintEffectNoSchedule,
			},
		}}}

	assert.False(t, tolerates(&api.Job{PodSpec: &v1.PodSpec{}}, n))
	assert.True(t, tolerates(job1, n))
}

func makeResourceList(cores int64, gigabytesRam int64) common.ComputeResources {
	cpuResource := resource.NewQuantity(cores, resource.DecimalSI)
	memoryResource := resource.NewQuantity(gigabytesRam*1024*1024*1024, resource.DecimalSI)
	resourceMap := common.ComputeResources{
		string(v1.ResourceCPU):    *cpuResource,
		string(v1.ResourceMemory): *memoryResource,
	}
	return resourceMap
}
