package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/G-Research/armada/pkg/api"
)

// TODO: Tests we should add:
// - Three queues. Check that all get about 1/3 of resources.
// - Two queues with factors 1 and 2. Check that one gets about 1/3 and the other about 2/3.
// - Scheduling from one queue with taints and toleration. Ensure those can/can't be scheduled accordingly.
// - One queue where the highest-priority jobs can't be scheduled. That we schedule jobs after those.
// - Respect QueueLeaseBatchSize (i.e., max number of jobs per queue to load per invocation of the scheduler).
// - Respect MaximalClusterFractionToSchedule (i.e., max fraction of total cluster resources to schedule per invocation of the scheduler).
// - Respect MaximalResourceFractionToSchedulePerQueue (i.e., max fraction of total cluster resources to schedule per queue per invocation of the scheduler).
// - Respect MaximalResourceFractionPerQueue (i.e., max fraction of all resources a single queue can obtain).
// - Respect MaximumJobsToSchedule (i.e., max number of jobs to schedule per invocation of the scheduler).
// - Test that we correctly account for init container resource requirements.
// Random with known seed for repeatable tests
var random = util.NewThreadsafeRand(42)

func TestQueuedJobsIterator_OneQueue(t *testing.T) {
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testNSmallCpuJob(0, 10) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	ctx := context.Background()
	it, err := NewQueuedJobsIterator(ctx, "A", repo)
	if !assert.NoError(t, err) {
		return
	}
	actual := make([]string, 0)
	for job, err := it.Next(); job != nil; job, err = it.Next() {
		if !assert.NoError(t, err) {
			return
		}
		actual = append(actual, job.Id)
	}
	assert.Equal(t, expected, actual)
}

func TestQueuedJobsIterator_ExceedsBufferSize(t *testing.T) {
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testNSmallCpuJob(0, 17) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	ctx := context.Background()
	it, err := NewQueuedJobsIterator(ctx, "A", repo)
	if !assert.NoError(t, err) {
		return
	}
	actual := make([]string, 0)
	for job, err := it.Next(); job != nil; job, err = it.Next() {
		if !assert.NoError(t, err) {
			return
		}
		actual = append(actual, job.Id)
	}
	assert.Equal(t, expected, actual)
}

func TestQueuedJobsIterator_ManyJobs(t *testing.T) {
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testNSmallCpuJob(0, 113) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	ctx := context.Background()
	it, err := NewQueuedJobsIterator(ctx, "A", repo)
	if !assert.NoError(t, err) {
		return
	}
	actual := make([]string, 0)
	for job, err := it.Next(); job != nil; job, err = it.Next() {
		if !assert.NoError(t, err) {
			return
		}
		actual = append(actual, job.Id)
	}
	assert.Equal(t, expected, actual)
}

func TestCreateQueuedJobsIterator_TwoQueues(t *testing.T) {
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testNSmallCpuJob(0, 10) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	for _, req := range testNSmallCpuJob(0, 10) {
		job := apiJobFromPodSpec("B", podSpecFromPodRequirements(req))
		repo.Enqueue(job)
	}

	ctx := context.Background()
	it, err := NewQueuedJobsIterator(ctx, "A", repo)
	if !assert.NoError(t, err) {
		return
	}
	actual := make([]string, 0)
	for job, err := it.Next(); job != nil; job, err = it.Next() {
		if !assert.NoError(t, err) {
			return
		}
		actual = append(actual, job.Id)
	}
	assert.Equal(t, expected, actual)
}

func TestCreateQueuedJobsIterator_RespectsTimeout(t *testing.T) {
	repo := newMockJobRepository()
	for _, req := range testNSmallCpuJob(0, 10) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	it, err := NewQueuedJobsIterator(ctx, "A", repo)
	if !assert.NoError(t, err) {
		return
	}
	job, err := it.Next()
	assert.Nil(t, job)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Calling again should produce the same error.
	job, err = it.Next()
	assert.Nil(t, job)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCreateQueuedJobsIterator_NilOnEmpty(t *testing.T) {
	repo := newMockJobRepository()
	for _, req := range testNSmallCpuJob(0, 10) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
	}

	ctx := context.Background()
	it, err := NewQueuedJobsIterator(ctx, "A", repo)
	if !assert.NoError(t, err) {
		return
	}
	for job, err := it.Next(); job != nil; job, err = it.Next() {
		if !assert.NoError(t, err) {
			return
		}
	}
	job, err := it.Next()
	assert.Nil(t, job)
	assert.NoError(t, err)
}

func TestQueueCandidateJobsIterator(t *testing.T) {
	tests := map[string]struct {
		Reqs                  []*schedulerobjects.PodRequirements
		Nodes                 []*schedulerobjects.Node
		InitialQueueResources schedulerobjects.ResourceList
		SchedulingConfig      configuration.SchedulingConfig
		MinimumJobSize        map[string]resource.Quantity
		// If true, jobs are actually leased.
		LeaseJobs bool
		// Indices of the reqs expected to be returned.
		ExpectedIndices []int
	}{
		"all jobs schedulable": {
			Reqs:            testNSmallCpuJob(0, 3),
			ExpectedIndices: []int{0, 1, 2},
		},
		"minimum job size (below limit)": {
			Reqs: append(testNSmallCpuJob(0, 3), testNLargeCpuJob(0, 2)...),
			MinimumJobSize: map[string]resource.Quantity{
				"cpu": resource.MustParse("31"),
			},
			ExpectedIndices: []int{3, 4},
		},
		"minimum job size (at limit)": {
			Reqs: append(testNSmallCpuJob(0, 3), testNLargeCpuJob(0, 2)...),
			MinimumJobSize: map[string]resource.Quantity{
				"cpu": resource.MustParse("32"),
			},
			ExpectedIndices: []int{3, 4},
		},
		"per-invocation scheduling limits": {
			Reqs:  testNSmallCpuJob(0, 3),
			Nodes: testNCpuNode(1, testPriorities),
			SchedulingConfig: configuration.SchedulingConfig{
				MaximalResourceFractionToSchedulePerQueue: common.ComputeResourcesFloat{
					"cpu": 2.0 / 32.0,
				},
			},
			LeaseJobs:       true,
			ExpectedIndices: []int{0, 1},
		},
		"total scheduling limits": {
			Reqs:  testNSmallCpuJob(0, 3),
			Nodes: testNCpuNode(1, testPriorities),
			SchedulingConfig: configuration.SchedulingConfig{
				MaximalResourceFractionPerQueue: common.ComputeResourcesFloat{
					"cpu": 2.0 / 32.0,
				},
			},
			LeaseJobs:       true,
			ExpectedIndices: []int{0, 1},
		},
		"total scheduling limits with initial usage": {
			Reqs:  testNSmallCpuJob(0, 3),
			Nodes: testNCpuNode(1, testPriorities),
			InitialQueueResources: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu": resource.MustParse("1"),
				},
			},
			SchedulingConfig: configuration.SchedulingConfig{
				MaximalResourceFractionPerQueue: common.ComputeResourcesFloat{
					"cpu": 2.0 / 32.0,
				},
			},
			LeaseJobs:       true,
			ExpectedIndices: []int{0},
		},
		"no resources for jobs": {
			Reqs:            testNSmallCpuJob(0, 3),
			Nodes:           testNCpuNode(0, testPriorities),
			ExpectedIndices: []int{},
		},
		"resources for 2 jobs": {
			Reqs:            testNLargeCpuJob(0, 3),
			Nodes:           testNCpuNode(2, testPriorities),
			LeaseJobs:       true,
			ExpectedIndices: []int{0, 1},
		},
		"resources for some jobs": {
			Reqs:            append(append(testNSmallCpuJob(0, 2), testNLargeCpuJob(0, 2)...), testNSmallCpuJob(0, 1)...),
			Nodes:           testNCpuNode(2, testPriorities),
			LeaseJobs:       true,
			ExpectedIndices: []int{0, 1, 2, 4},
		},
		"preemption": {
			Reqs:            append(append(testNLargeCpuJob(0, 2), testNLargeCpuJob(0, 2)...), testNSmallCpuJob(1, 1)...),
			Nodes:           testNCpuNode(2, testPriorities),
			LeaseJobs:       true,
			ExpectedIndices: []int{0, 1, 4},
		},
		"tainted CPU nodes": {
			Reqs:            append(testNSmallCpuJob(0, 1), testNLargeCpuJob(0, 1)...),
			Nodes:           testNTaintedCpuNode(1, testPriorities),
			LeaseJobs:       true,
			ExpectedIndices: []int{1},
		},
		"GPU nodes": {
			Reqs:            append(testNSmallCpuJob(0, 1), testNGPUJob(0, 8)...),
			Nodes:           testNTaintedtGpuNode(1, testPriorities),
			LeaseJobs:       true,
			ExpectedIndices: []int{1, 2, 3, 4, 5, 6, 7, 8},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {

			repo := newMockJobRepository()
			jobs := make([]*api.Job, len(tc.Reqs))
			for i, req := range tc.Reqs { // Queue name doesn't matter.
				jobs[i] = apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
				repo.Enqueue(jobs[i])
			}

			expected := make([]*api.Job, len(tc.ExpectedIndices))
			for i, j := range tc.ExpectedIndices {
				expected[i] = jobs[j]
			}

			scheduler := LegacyScheduler{
				SchedulingConfig: tc.SchedulingConfig,
				JobRepository:    repo,
				MinimumJobSize:   tc.MinimumJobSize,
			}
			if tc.Nodes != nil {
				nodeDb, err := NewNodeDb(testPriorities, testResources)
				if !assert.NoError(t, err) {
					return
				}
				err = nodeDb.Upsert(tc.Nodes)
				if !assert.NoError(t, err) {
					return
				}
				scheduler.NodeDb = nodeDb
			}

			ctx := context.Background()
			it, err := NewQueueCandidateJobsIterator(ctx, "A", tc.InitialQueueResources, scheduler)
			if !assert.NoError(t, err) {
				return
			}
			actual := make([]*api.Job, 0)
			for report, err := it.Next(); report != nil; report, err = it.Next() {
				if !assert.NoError(t, err) {
					return
				}
				if tc.LeaseJobs {
					it.Lease(report)
				}
				actual = append(actual, report.Job)
			}
			assert.Equal(t, expected, actual)
		})
	}
}

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
	scheduler.Rand = random

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

func testSchedulingConfig() configuration.SchedulingConfig {
	priorityClasses := make(map[string]int32)
	for _, priority := range testPriorities {
		priorityClasses[fmt.Sprintf("%d", priority)] = priority
	}
	return configuration.SchedulingConfig{
		Preemption: configuration.PreemptionConfig{
			PriorityClasses: priorityClasses,
		},
		IndexedResources: []string{"cpu", "memory"},
	}
}

func TestSchedule(t *testing.T) {
	tests := map[string]struct {
		SchedulingConfig configuration.SchedulingConfig
		// Nodes to be considred by the scheduler.
		Nodes []*schedulerobjects.Node
		// Map from queue name to pod requirements for that queue.
		ReqsByQueue map[string][]*schedulerobjects.PodRequirements
		// Map from queue to the priority factor associated with that queue.
		PriorityFactorsByQueue map[string]float64
		// For each queue, the indices of jobs expected to be scheduled.
		ExpectedIndicesByQueue map[string][]int
		// For each queue, the expected resources assigned to jobs from that queue.
		ExpectedResourcesByQueue map[string]resourceLimits
	}{}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobRepository := newMockJobRepository()
			for queue, reqs := range tc.ReqsByQueue {
				jobRepository.EnqueueMany(apiJobsFromPodReqs(queue, reqs))
			}

			expectedByQueue := make(map[string][]string)
			for queue, jobs := range jobRepository.jobsByQueue {
				is := tc.ExpectedIndicesByQueue[queue]
				expected := make([]string, len(is))
				for i, j := range is {
					expected[i] = jobs[j].Id
				}
				expectedByQueue[queue] = expected
			}

			scheduler, err := NewLegacyScheduler(
				tc.SchedulingConfig,
				"executor",
				tc.Nodes,
				jobRepository,
				tc.PriorityFactorsByQueue,
			)
			if !assert.NoError(t, err) {
				return
			}
			scheduler.Rand = random

			jobs, err := scheduler.Schedule(context.Background(), nil)
			if !assert.NoError(t, err) {
				return
			}

			if len(tc.ExpectedIndicesByQueue) != 0 {
				actualScheduledJobsByQueue := jobIdsByQueueFromJobs(jobs)
				for queue, expected := range expectedByQueue {
					actual := actualScheduledJobsByQueue[queue]
					assert.Equal(t, expected, actual, "queue %s", queue)
				}
			}

		})
	}
}

func TestSchedule_OneJobNew(t *testing.T) {
	jobRepository := newMockJobRepository()
	jobRepository.EnqueueMany(apiJobsFromPodReqs("A", testNSmallCpuJob(0, 1)))

	expectedIndicesByQueue := map[string][]int{
		"A": {0},
	}
	expectedByQueue := make(map[string][]string)
	for queue, jobs := range jobRepository.jobsByQueue {
		is := expectedIndicesByQueue[queue]
		expected := make([]string, len(is))
		for i, j := range is {
			expected[i] = jobs[j].Id
		}
		expectedByQueue[queue] = expected
	}

	scheduler, err := NewLegacyScheduler(
		testSchedulingConfig(),
		"executor",
		testNCpuNode(1, testPriorities),
		jobRepository,
		map[string]float64{
			"A": 1,
		},
	)
	scheduler.Rand = random
	if !assert.NoError(t, err) {
		return
	}

	fmt.Println("scheduling")
	jobs, err := scheduler.Schedule(context.Background(), nil)
	if !assert.NoError(t, err) {
		return
	}
	fmt.Println("scheduling finished")

	actualScheduledJobsByQueue := jobIdsByQueueFromJobs(jobs)
	for queue, expected := range expectedByQueue {
		actual := actualScheduledJobsByQueue[queue]
		assert.Equal(t, expected, actual, "queue %s", queue)
	}

	// if expectedResourcesByQueue != nil {
	// 	actualUsageByQueue := usageByQueue(jobs)
	// 	for queue, usage := range actualUsageByQueue {
	// 		fmt.Printf("%s usage: %v\n", queue, usage)
	// 		assertResourceLimitsSatisfied(t, expectedResourcesByQueue[queue], usage)
	// 	}
	// }

}

func apiJobsFromPodReqs(queue string, reqs []*schedulerobjects.PodRequirements) []*api.Job {
	rv := make([]*api.Job, len(reqs))
	for i, req := range reqs {
		rv[i] = apiJobFromPodSpec(queue, podSpecFromPodRequirements(req))
	}
	return rv
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
		Rand:                  random,
	}

	jobsByQueue := map[string][]*api.Job{
		"A": {
			apiJobFromPodSpec("A", podSpecFromPodRequirements(testSmallCpuJob(0))),
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
		Rand:                  random,
	}

	jobsByQueue := map[string][]*api.Job{}
	for _, req := range testNSmallCpuJob(0, 10) {
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
		Rand:                  random,
	}

	jobsByQueue := map[string][]*api.Job{}
	for _, req := range testNSmallCpuJob(0, 10) {
		jobsByQueue["A"] = append(
			jobsByQueue["A"],
			apiJobFromPodSpec("A", podSpecFromPodRequirements(req)),
		)
	}
	for _, req := range testNSmallCpuJob(0, 10) {
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
	for _, req := range testNSmallCpuJob(0, 20) {
		jobsByQueue["A"] = append(
			jobsByQueue["A"],
			apiJobFromPodSpec("A", podSpecFromPodRequirements(req)),
		)
	}
	for _, req := range testNSmallCpuJob(0, 20) {
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
				"cpu": resource.MustParse("20"),
			},
			map[string]resource.Quantity{
				"cpu": resource.MustParse("21"),
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
	for _, req := range testNSmallCpuJob(0, 40) {
		jobsByQueue["A"] = append(
			jobsByQueue["A"],
			apiJobFromPodSpec("A", podSpecFromPodRequirements(req)),
		)
	}
	for _, req := range testNSmallCpuJob(0, 40) {
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
		Rand:                  random,
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
func TestSchedule_TwoQueueFairnessWithIntial(t *testing.T) {
	// Create a cluster with 32 cores.
	nodes := []*schedulerobjects.Node{
		testCpuNode(testPriorities),
	}

	// Two queues with 20 single-core jobs each.
	jobsByQueue := map[string][]*api.Job{}
	for _, req := range testNSmallCpuJob(0, 20) {
		jobsByQueue["A"] = append(
			jobsByQueue["A"],
			apiJobFromPodSpec("A", podSpecFromPodRequirements(req)),
		)
	}
	for _, req := range testNSmallCpuJob(0, 20) {
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
		Rand:                  random,
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
			apiJobFromPodSpec("A", podSpecFromPodRequirements(testSmallCpuJob(0))),
		},
	}

	scheduler := &LegacyScheduler{
		SchedulingConfig:      configuration.SchedulingConfig{},
		ExecutorId:            "executor",
		MinimumJobSize:        make(map[string]resource.Quantity),
		PriorityFactorByQueue: map[string]float64{"A": 1, "B": 1},
		Rand:                  random,
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

// Test that a user can preempt another user's jobs up to their fair share
// TODO:  Why is this failing
func TestSchedule_PreemptOtherUser(t *testing.T) {

	// Create a cluster with 32 cores.
	nodes := []*schedulerobjects.Node{
		testCpuNode(testPriorities),
	}

	// Queue B has 32 jobs of one core each
	jobsByQueue := map[string][]*api.Job{}
	for _, req := range testNSmallCpuJob(0, 32) {
		job := podSpecFromPodRequirements(req)
		//job.Priority = pointer.Int32(1) // set a higher priority than queue a's jobs
		jobsByQueue["B"] = append(
			jobsByQueue["B"],
			apiJobFromPodSpec("B", job),
		)
	}

	scheduler := &LegacyScheduler{
		SchedulingConfig:      configuration.SchedulingConfig{},
		ExecutorId:            "executor",
		MinimumJobSize:        make(map[string]resource.Quantity),
		PriorityFactorByQueue: map[string]float64{"A": 1, "B": 1},
		Rand:                  random,
	}

	expectedResourcesByQueue := map[string]resourceLimits{
		"A": newResourceLimits(
			map[string]resource.Quantity{
				"cpu": resource.MustParse("18"),
			},
			map[string]resource.Quantity{
				"cpu": resource.MustParse("14"),
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
					"cpu": resource.MustParse("32"),
				},
			},
		},
		"B": {},
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
	for _, req := range testNSmallCpuJob(0, 20) {
		jobsByQueue["A"] = append(
			jobsByQueue["A"],
			apiJobFromPodSpec("A", podSpecFromPodRequirements(req)),
		)
	}
	for _, req := range testNSmallCpuJob(0, 20) {
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
		Rand:                  random,
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

type mockJobRepository struct {
	jobsByQueue         map[string][]*api.Job
	jobsById            map[string]*api.Job
	getQueueJobIdsDelay time.Duration
}

func newMockJobRepository() *mockJobRepository {
	return &mockJobRepository{
		jobsByQueue: make(map[string][]*api.Job),
		jobsById:    make(map[string]*api.Job),
	}
}

func (repo *mockJobRepository) EnqueueMany(jobs []*api.Job) {
	for _, job := range jobs {
		repo.Enqueue(job)
	}
}

func (repo *mockJobRepository) Enqueue(job *api.Job) {
	repo.jobsByQueue[job.Queue] = append(repo.jobsByQueue[job.Queue], job)
	repo.jobsById[job.Id] = job
}

func (repo *mockJobRepository) GetQueueJobIds(queue string) ([]string, error) {
	time.Sleep(repo.getQueueJobIdsDelay)
	if jobs, ok := repo.jobsByQueue[queue]; ok {
		rv := make([]string, len(jobs))
		for i, job := range jobs {
			rv[i] = job.Id
		}
		return rv, nil
	} else {
		return make([]string, 0), nil
	}
}

func (repo *mockJobRepository) GetExistingJobsByIds(jobIds []string) ([]*api.Job, error) {
	rv := make([]*api.Job, len(jobIds))
	for i, jobId := range jobIds {
		if job, ok := repo.jobsById[jobId]; ok {
			rv[i] = job
		}
	}
	return rv, nil
}

func (repo *mockJobRepository) TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error) {
	remainingJobs := []*api.Job{}
outer:
	for _, j := range repo.jobsByQueue[queue] {
		for _, l := range jobs {
			if j == l {
				continue outer
			}
		}
		remainingJobs = append(remainingJobs, j)
	}
	repo.jobsByQueue[queue] = remainingJobs
	return jobs, nil
}
