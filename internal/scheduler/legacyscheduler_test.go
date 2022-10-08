package scheduler

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/G-Research/armada/pkg/api"
)

func testSchedule(
	nodes []*schedulerobjects.Node,
	jobsByQueue map[string][]*api.Job,
	initialUsageByQueue map[string]schedulerobjects.QuantityByPriorityAndResourceType,
	scheduler *LegacyScheduler,
	expectedScheduledJobsByQueue map[string][]string,
	expectedResourcesByQueue map[string]resourceLimits,
	t *testing.T) {

	nodeDb, err := NewNodeDb(testPriorities, testResources)
	if !assert.NoError(t, err) {
		return
	}
	err = nodeDb.Upsert(nodes)
	if !assert.NoError(t, err) {
		return
	}
	scheduler.NodeDb = nodeDb

	jobQueue := newFakeJobQueue()
	jobQueue.jobsByQueue = jobsByQueue
	scheduler.JobQueue = jobQueue

	jobs, err := scheduler.Schedule(context.Background(), initialUsageByQueue)
	if !assert.NoError(t, err) {
		return
	}

	actualScheduledJobsByQueue := jobIdsByQueueFromJobs(jobs)
	if expectedScheduledJobsByQueue != nil {
		assert.Equal(t, expectedScheduledJobsByQueue, actualScheduledJobsByQueue)
	}
	if expectedResourcesByQueue != nil {
		actualUsageByQueue := usageByQueue(jobs)
		for queue, usage := range actualUsageByQueue {
			fmt.Printf("%s usage: %v\n", queue, usage)
			assertResourceLimitsSatisfied(t, expectedResourcesByQueue[queue], usage)
		}
	}

}

// Test schedule one job from one queue.
func TestSchedule_OneJob(t *testing.T) {
	nodes := []*schedulerobjects.Node{
		testCpuNode(testPriorities),
	}
	scheduler := &LegacyScheduler{
		SchedulingConfig:      configuration.SchedulingConfig{},
		ExecutorId:            "executor",
		MinimumJobSize:        make(map[string]resource.Quantity),
		PriorityFactorByQueue: map[string]float64{"A": 1},
	}

	jobsByQueue := map[string][]*api.Job{
		"A": {
			apiJobFromPodSpec("A", podSpecFromPodRequirements(testSmallCpuJob())),
		},
	}
	expectedScheduledJobs := jobIdsByQueueFromJobs(jobsByQueue["A"])

	initialUsageByQueue := map[string]schedulerobjects.QuantityByPriorityAndResourceType{
		"A": {
			0: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu": resource.MustParse("1"),
				},
			},
		},
	}

	testSchedule(nodes, jobsByQueue, initialUsageByQueue, scheduler, expectedScheduledJobs, nil, t)
}

// Test scheduling several jobs from one queue.
func TestSchedule_OneQueue(t *testing.T) {
	nodes := []*schedulerobjects.Node{
		testCpuNode(testPriorities),
	}
	scheduler := &LegacyScheduler{
		SchedulingConfig:      configuration.SchedulingConfig{},
		ExecutorId:            "executor",
		MinimumJobSize:        make(map[string]resource.Quantity),
		PriorityFactorByQueue: map[string]float64{"A": 1},
	}

	jobsByQueue := map[string][]*api.Job{}
	for _, req := range testNSmallCpuJob(10) {
		jobsByQueue["A"] = append(
			jobsByQueue["A"],
			apiJobFromPodSpec("A", podSpecFromPodRequirements(req)),
		)
	}

	expectedScheduledJobs := make([]*api.Job, 0)
	for _, jobs := range jobsByQueue {
		expectedScheduledJobs = append(expectedScheduledJobs, jobs...)
	}
	expectedScheduledJobsByQueue := jobIdsByQueueFromJobs(expectedScheduledJobs)

	initialUsageByQueue := map[string]schedulerobjects.QuantityByPriorityAndResourceType{
		"A": {},
	}

	testSchedule(nodes, jobsByQueue, initialUsageByQueue, scheduler, expectedScheduledJobsByQueue, nil, t)
}

// Test scheduling several jobs from two queues.
func TestSchedule_TwoQueues(t *testing.T) {
	nodes := []*schedulerobjects.Node{
		testCpuNode(testPriorities),
	}
	scheduler := &LegacyScheduler{
		SchedulingConfig:      configuration.SchedulingConfig{},
		ExecutorId:            "executor",
		MinimumJobSize:        make(map[string]resource.Quantity),
		PriorityFactorByQueue: map[string]float64{"A": 1, "B": 1},
	}

	jobsByQueue := map[string][]*api.Job{}
	for _, req := range testNSmallCpuJob(10) {
		jobsByQueue["A"] = append(
			jobsByQueue["A"],
			apiJobFromPodSpec("A", podSpecFromPodRequirements(req)),
		)
	}
	for _, req := range testNSmallCpuJob(10) {
		jobsByQueue["B"] = append(
			jobsByQueue["B"],
			apiJobFromPodSpec("B", podSpecFromPodRequirements(req)),
		)
	}

	expectedScheduledJobs := make([]*api.Job, 0)
	for _, jobs := range jobsByQueue {
		expectedScheduledJobs = append(expectedScheduledJobs, jobs...)
	}
	expectedScheduledJobsByQueue := jobIdsByQueueFromJobs(expectedScheduledJobs)

	initialUsageByQueue := map[string]schedulerobjects.QuantityByPriorityAndResourceType{
		"A": {},
		"B": {},
	}

	testSchedule(nodes, jobsByQueue, initialUsageByQueue, scheduler, expectedScheduledJobsByQueue, nil, t)
}

// Test that two queues with equal weights are equally allocated jobs equally
func TestSchedule_TwoQueueFairness(t *testing.T) {

	// Create a cluster with 32 cores.
	nodes := []*schedulerobjects.Node{
		testCpuNode(testPriorities),
	}

	// Two queues with 20 single-core jobs each.
	jobsByQueue := map[string][]*api.Job{}
	for _, req := range testNSmallCpuJob(20) {
		jobsByQueue["A"] = append(
			jobsByQueue["A"],
			apiJobFromPodSpec("A", podSpecFromPodRequirements(req)),
		)
	}
	for _, req := range testNSmallCpuJob(20) {
		jobsByQueue["B"] = append(
			jobsByQueue["B"],
			apiJobFromPodSpec("B", podSpecFromPodRequirements(req)),
		)
	}

	scheduler := &LegacyScheduler{
		SchedulingConfig:      configuration.SchedulingConfig{},
		ExecutorId:            "executor",
		MinimumJobSize:        make(map[string]resource.Quantity),
		PriorityFactorByQueue: map[string]float64{"A": 1, "B": 1},
	}

	expectedResourcesByQueue := map[string]resourceLimits{
		"A": newResourceLimits(
			map[string]resource.Quantity{
				"cpu": resource.MustParse("14"),
			},
			map[string]resource.Quantity{
				"cpu": resource.MustParse("18"),
			},
		),
		"B": newResourceLimits(
			map[string]resource.Quantity{
				"cpu": resource.MustParse("14"),
			},
			map[string]resource.Quantity{
				"cpu": resource.MustParse("18"),
			},
		),
	}

	initialUsageByQueue := map[string]schedulerobjects.QuantityByPriorityAndResourceType{
		"A": {},
		"B": {},
	}

	testSchedule(nodes, jobsByQueue, initialUsageByQueue, scheduler, nil, expectedResourcesByQueue, t)
}

// Test that two queues with unequal weights are allocated jobs according to the weighting
func TestSchedule_TwoQueueFairnessDifferentPriorities(t *testing.T) {

	// Create a cluster with 32 cores.
	nodes := []*schedulerobjects.Node{
		testCpuNode(testPriorities),
	}

	// Two queues with 40 single-core jobs each.
	jobsByQueue := map[string][]*api.Job{}
	for _, req := range testNSmallCpuJob(40) {
		jobsByQueue["A"] = append(
			jobsByQueue["A"],
			apiJobFromPodSpec("A", podSpecFromPodRequirements(req)),
		)
	}
	for _, req := range testNSmallCpuJob(40) {
		jobsByQueue["B"] = append(
			jobsByQueue["B"],
			apiJobFromPodSpec("B", podSpecFromPodRequirements(req)),
		)
	}

	scheduler := &LegacyScheduler{
		SchedulingConfig:      configuration.SchedulingConfig{},
		ExecutorId:            "executor",
		MinimumJobSize:        make(map[string]resource.Quantity),
		PriorityFactorByQueue: map[string]float64{"A": 3, "B": 1},
	}

	expectedResourcesByQueue := map[string]resourceLimits{
		"A": newResourceLimits(
			map[string]resource.Quantity{
				"cpu": resource.MustParse("23"),
			},
			map[string]resource.Quantity{
				"cpu": resource.MustParse("25"),
			},
		),
		"B": newResourceLimits(
			map[string]resource.Quantity{
				"cpu": resource.MustParse("7"),
			},
			map[string]resource.Quantity{
				"cpu": resource.MustParse("9"),
			},
		),
	}

	initialUsageByQueue := map[string]schedulerobjects.QuantityByPriorityAndResourceType{
		"A": {},
		"B": {},
	}

	testSchedule(nodes, jobsByQueue, initialUsageByQueue, scheduler, nil, expectedResourcesByQueue, t)

}

// Test that two queues with equal weights have fair share if one queue already has jobs
func TestSchedule_TwoQueueFairnessWIthIntial(t *testing.T) {
	// Create a cluster with 32 cores.
	nodes := []*schedulerobjects.Node{
		testCpuNode(testPriorities),
	}

	// Two queues with 20 single-core jobs each.
	jobsByQueue := map[string][]*api.Job{}
	for _, req := range testNSmallCpuJob(20) {
		jobsByQueue["A"] = append(
			jobsByQueue["A"],
			apiJobFromPodSpec("A", podSpecFromPodRequirements(req)),
		)
	}
	for _, req := range testNSmallCpuJob(20) {
		jobsByQueue["B"] = append(
			jobsByQueue["B"],
			apiJobFromPodSpec("B", podSpecFromPodRequirements(req)),
		)
	}

	scheduler := &LegacyScheduler{
		SchedulingConfig:      configuration.SchedulingConfig{},
		ExecutorId:            "executor",
		MinimumJobSize:        make(map[string]resource.Quantity),
		PriorityFactorByQueue: map[string]float64{"A": 1, "B": 1},
	}

	expectedResourcesByQueue := map[string]resourceLimits{
		"A": newResourceLimits(
			map[string]resource.Quantity{
				"cpu": resource.MustParse("14"),
			},
			map[string]resource.Quantity{
				"cpu": resource.MustParse("18"),
			},
		),
		"B": newResourceLimits(
			map[string]resource.Quantity{
				"cpu": resource.MustParse("14"),
			},
			map[string]resource.Quantity{
				"cpu": resource.MustParse("18"),
			},
		),
	}

	initialUsageByQueue := map[string]schedulerobjects.QuantityByPriorityAndResourceType{
		"A": {
			0: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu": resource.MustParse("16"),
				},
			},
		},
		"B": {},
	}

	testSchedule(nodes, jobsByQueue, initialUsageByQueue, scheduler, nil, expectedResourcesByQueue, t)
}

// Test that no jobs are scheduled if there aren't enough resources
func TestSchedule_CantSchedule(t *testing.T) {

	// Create a cluster with 32 cores.
	nodes := []*schedulerobjects.Node{
		testCpuNode(testPriorities),
	}

	jobsByQueue := map[string][]*api.Job{
		"A": {
			apiJobFromPodSpec("A", podSpecFromPodRequirements(testSmallCpuJob())),
		},
	}

	scheduler := &LegacyScheduler{
		SchedulingConfig:      configuration.SchedulingConfig{},
		ExecutorId:            "executor",
		MinimumJobSize:        make(map[string]resource.Quantity),
		PriorityFactorByQueue: map[string]float64{"A": 1, "B": 1},
	}

	expectedResourcesByQueue := map[string]resourceLimits{
		"A": newResourceLimits(
			map[string]resource.Quantity{
				"cpu": resource.MustParse("0"),
			},
			map[string]resource.Quantity{
				"cpu": resource.MustParse("0"),
			},
		),
	}

	initialUsageByQueue := map[string]schedulerobjects.QuantityByPriorityAndResourceType{
		"B": {
			0: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu": resource.MustParse("32"),
				},
			},
		},
	}
	testSchedule(nodes, jobsByQueue, initialUsageByQueue, scheduler, nil, expectedResourcesByQueue, t)
}

type resourceLimits struct {
	Minimum schedulerobjects.ResourceList
	Maximum schedulerobjects.ResourceList
}

func newResourceLimits(minimum map[string]resource.Quantity, maximum map[string]resource.Quantity) resourceLimits {
	return resourceLimits{
		Minimum: schedulerobjects.ResourceList{Resources: minimum},
		Maximum: schedulerobjects.ResourceList{Resources: maximum},
	}
}

func assertResourceLimitsSatisfied(t *testing.T, limits resourceLimits, resources schedulerobjects.ResourceList) bool {
	for resource, min := range limits.Minimum.Resources {
		actual := resources.Resources[resource]
		if !assert.NotEqual(t, 1, min.Cmp(actual), "%s limits not satisfied: min is %s, but actual is %s", resource, min.String(), actual.String()) {
			return false
		}
	}
	for resource, actual := range resources.Resources {
		if max, ok := limits.Maximum.Resources[resource]; ok {
			if !assert.NotEqual(t, -1, max.Cmp(actual), "%s limits not satisfied: max is %s, but actual is %s", resource, max.String(), actual.String()) {
				return false
			}
		}
	}
	return true
}

// Test that big jobs that cannot be scheduled, don't block small jobs that can
func TestSchedule_BigJobDoesBlockSmallJobs(t *testing.T) {
	// Create a cluster with 32 cores.
	nodes := []*schedulerobjects.Node{
		testCpuNode(testPriorities),
	}

	jobsByQueue := map[string][]*api.Job{}

	// Give each queue an unschedulable job
	jobsByQueue["A"] = append(
		jobsByQueue["A"],
		apiJobFromPodSpec("A", podSpecFromPodRequirements(&schedulerobjects.PodRequirements{
			Priority: 0,
			ResourceRequirements: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    resource.MustParse("100"),
					"memory": resource.MustParse("4Gi"),
				},
			},
		})),
	)

	jobsByQueue["B"] = append(
		jobsByQueue["B"],
		apiJobFromPodSpec("B", podSpecFromPodRequirements(&schedulerobjects.PodRequirements{
			Priority: 0,
			ResourceRequirements: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    resource.MustParse("100"),
					"memory": resource.MustParse("4Gi"),
				},
			},
		})),
	)

	// Add 20 single-core jobs each to each queue
	for _, req := range testNSmallCpuJob(20) {
		jobsByQueue["A"] = append(
			jobsByQueue["A"],
			apiJobFromPodSpec("A", podSpecFromPodRequirements(req)),
		)
	}
	for _, req := range testNSmallCpuJob(20) {
		jobsByQueue["B"] = append(
			jobsByQueue["B"],
			apiJobFromPodSpec("B", podSpecFromPodRequirements(req)),
		)
	}

	scheduler := &LegacyScheduler{
		SchedulingConfig:      configuration.SchedulingConfig{},
		ExecutorId:            "executor",
		MinimumJobSize:        make(map[string]resource.Quantity),
		PriorityFactorByQueue: map[string]float64{"A": 1, "B": 1},
	}

	expectedResourcesByQueue := map[string]resourceLimits{
		"A": newResourceLimits(
			map[string]resource.Quantity{
				"cpu": resource.MustParse("14"),
			},
			map[string]resource.Quantity{
				"cpu": resource.MustParse("18"),
			},
		),
		"B": newResourceLimits(
			map[string]resource.Quantity{
				"cpu": resource.MustParse("14"),
			},
			map[string]resource.Quantity{
				"cpu": resource.MustParse("18"),
			},
		),
	}

	initialUsageByQueue := map[string]schedulerobjects.QuantityByPriorityAndResourceType{
		"A": {
			0: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu": resource.MustParse("16"),
				},
			},
		},
		"B": {},
	}

	testSchedule(nodes, jobsByQueue, initialUsageByQueue, scheduler, nil, expectedResourcesByQueue, t)
}

func (limits resourceLimits) areSatisfied(resources schedulerobjects.ResourceList) bool {
	for t, q := range limits.Minimum.Resources {
		min := resources.Resources[t]
		if min.Cmp(q) == -1 {
			return false
		}
	}
	for t, q := range resources.Resources {
		if max, ok := limits.Maximum.Resources[t]; ok {
			if max.Cmp(q) == 1 {
				return false
			}
		}
	}
	return true
}

// TODO: Test pods with large init containers (that we account for them).

// func TestScheduleOne(t *testing.T) {

// 	nodes := []*schedulerobjects.Node{
// 		testCpuNode(testPriorities),
// 	}
// 	nodeDb, err := NewNodeDb(testPriorities, testResources)
// 	if !assert.NoError(t, err) {
// 		return
// 	}
// 	err = nodeDb.Upsert(nodes)
// 	if !assert.NoError(t, err) {
// 		return
// 	}

// 	jobQueue := newFakeJobQueue()
// 	legacyScheduler := &LegacyScheduler{
// 		SchedulingConfig: configuration.SchedulingConfig{},
// 		ExecutorId:       "executor",
// 		NodeDb:           nodeDb,
// 		JobQueue:         jobQueue,
// 		MinimumJobSize:   make(map[string]resource.Quantity),
// 	}

// 	queue := "queue"
// 	jobQueue.jobsByQueue[queue] = []*api.Job{
// 		apiJobFromPodSpec(queue, podSpecFromPodRequirements(testSmallCpuJob())),
// 	}

// 	initialUsageByQueue := map[string]schedulerobjects.QuantityByPriorityAndResourceType{
// 		queue: {
// 			0: schedulerobjects.ResourceList{
// 				Resources: map[string]resource.Quantity{
// 					"cpu": resource.MustParse("1"),
// 				},
// 			},
// 		},
// 	}

// 	expectedScheduledJobs := jobIdsByQueueFromJobs(jobQueue.jobsByQueue[queue])

// 	jobs, err := legacyScheduler.Schedule(context.Background(), initialUsageByQueue)
// 	if !assert.NoError(t, err) {
// 		return
// 	}

// 	actualScheduledJobs := jobIdsByQueueFromJobs(jobs)

// 	assert.Equal(t, expectedScheduledJobs, actualScheduledJobs)
// 	assert.Equal(t, 1, len(legacyScheduler.JobSchedulingReportsByQueue))
// 	assert.Equal(t, 1, len(legacyScheduler.JobSchedulingReportsByQueue[queue]))

// 	// for _, reports := range legacyScheduler.JobSchedulingReportsByQueue {
// 	// 	for _, jobReport := range reports {
// 	// 		fmt.Println(jobReport)
// 	// 	}
// 	// }
// }

func jobIdsByQueueFromJobs(jobs []*api.Job) map[string][]string {
	rv := make(map[string][]string)
	for _, job := range jobs {
		rv[job.Queue] = append(rv[job.Queue], job.Id)
	}
	return rv
}

func usageByQueue(jobs []*api.Job) map[string]schedulerobjects.ResourceList {
	rv := make(map[string]schedulerobjects.ResourceList)
	for _, job := range jobs {
		rl, ok := rv[job.Queue]
		if !ok {
			rl = schedulerobjects.ResourceList{
				Resources: make(map[string]resource.Quantity),
			}
			rv[job.Queue] = rl
		}
		for t, q := range common.TotalJobResourceRequest(job) {
			quantity := rl.Resources[t]
			quantity.Add(q)
			rl.Resources[t] = quantity
		}
	}
	return rv
}

func apiJobFromPodSpec(queue string, podSpec *v1.PodSpec) *api.Job {
	return &api.Job{
		Id:      util.NewULID(),
		PodSpec: podSpec,
		Queue:   queue,
	}
}

func podSpecFromPodRequirements(req *schedulerobjects.PodRequirements) *v1.PodSpec {
	return &v1.PodSpec{
		NodeSelector:     req.NodeSelector,
		Affinity:         req.Affinity,
		Tolerations:      req.Tolerations,
		Priority:         &req.Priority,
		PreemptionPolicy: (*v1.PreemptionPolicy)(&req.PreemptionPolicy),
		Containers: []v1.Container{
			{
				Resources: req.ResourceRequirements,
			},
		},
	}
}

type fakeJobQueue struct {
	jobsByQueue map[string][]*api.Job
}

func newFakeJobQueue() *fakeJobQueue {
	return &fakeJobQueue{
		jobsByQueue: make(map[string][]*api.Job),
	}
}

func (r *fakeJobQueue) PeekClusterQueue(clusterId, queue string, limit int64) ([]*api.Job, error) {
	jobs := r.jobsByQueue[queue]
	fmt.Println("there are ", len(jobs), " jobs for queue ", queue)
	if len(jobs) == 0 {
		return make([]*api.Job, 0), nil
	}
	if int64(len(jobs)) > limit {
		return jobs[:limit], nil
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
