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

func testSchedulingConfig() configuration.SchedulingConfig {
	priorityClasses := make(map[string]int32)
	for _, priority := range testPriorities {
		priorityClasses[fmt.Sprintf("%d", priority)] = priority
	}
	return configuration.SchedulingConfig{
		ResourceScarcity: map[string]float64{"cpu": 1, "memory": 1},
		Preemption: configuration.PreemptionConfig{
			PriorityClasses: priorityClasses,
		},
		IndexedResources: []string{"cpu", "memory"},
	}
}

func withRoundLimits(limits map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximalClusterFractionToSchedule = limits
	return config
}

func withPerQueueLimits(limits map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximalResourceFractionPerQueue = limits
	return config
}

func withPerQueueRoundLimits(limits map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximalResourceFractionToSchedulePerQueue = limits
	return config
}

func withMaxJobsToSchedule(n int, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximumJobsToSchedule = n
	return config
}

func withUsedResources(p int32, rs schedulerobjects.ResourceList, nodes []*schedulerobjects.Node) []*schedulerobjects.Node {
	for _, node := range nodes {
		schedulerobjects.AvailableByPriorityAndResourceType(node.AvailableByPriorityAndResource).MarkUsed(p, rs)
	}
	return nodes
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
		// Initial resource usage for all queues.
		InitialUsageByQueue map[string]schedulerobjects.QuantityByPriorityAndResourceType
		// For each queue, the indices of jobs expected to be scheduled.
		ExpectedIndicesByQueue map[string][]int
		// For each queue, the expected resources assigned to jobs from that queue.
		ExpectedResourcesByQueue map[string]resourceLimits
	}{
		"one queue one job": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 1),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"one queue several jobs": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 32),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": intRange(0, 31),
			},
		},
		"one queue some jobs do not fit": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 33),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": intRange(0, 31),
			},
		},
		"one queue with jobs of varying size": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(2, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob(0, 32), testNLargeCpuJob(0, 1)...),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": intRange(0, 32),
			},
		},
		"preempt lower-priority jobs": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob(0, 1), testNLargeCpuJob(1, 1)...),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
			},
			// TODO: We could optimise this by un-leasing preempted jobs.
			ExpectedIndicesByQueue: map[string][]int{
				"A": intRange(0, 1),
			},
		},
		"no preemption of higher-priority jobs": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNLargeCpuJob(1, 1), testNSmallCpuJob(0, 1)...),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		// TODO: Also try with tainted nodes.
		"unschedulable jobs do not block schedulable jobs": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(append(testNSmallCpuJob(0, 1), testNLargeCpuJob(0, 10)...), testNSmallCpuJob(0, 1)...),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 11},
			},
		},
		"max jobs to schedule limit": {
			SchedulingConfig: withMaxJobsToSchedule(2, testSchedulingConfig()),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 5),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 1},
			},
		},
		"round limits": {
			SchedulingConfig: withRoundLimits(map[string]float64{"cpu": 2.0 / 32.0}, testSchedulingConfig()),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 5),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 1},
			},
		},
		"round per-queue limits": {
			SchedulingConfig: withPerQueueRoundLimits(map[string]float64{"cpu": 2.0 / 32.0}, testSchedulingConfig()),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 5),
				"B": testNSmallCpuJob(0, 5),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 1},
				"B": {0, 1},
			},
		},
		"overall per-queue limits": {
			SchedulingConfig: withPerQueueLimits(map[string]float64{"cpu": 2.0 / 32.0}, testSchedulingConfig()),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 5),
				"B": testNSmallCpuJob(0, 5),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 1},
				"B": {0, 1},
			},
		},
		"overall per-queue limits with initial usage": {
			SchedulingConfig: withPerQueueLimits(map[string]float64{"cpu": 2.0 / 32.0}, testSchedulingConfig()),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 5),
				"B": testNSmallCpuJob(0, 5),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
			InitialUsageByQueue: map[string]schedulerobjects.QuantityByPriorityAndResourceType{
				"A": {
					0: schedulerobjects.ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("0"),
						},
					},
				},
				"B": {
					0: schedulerobjects.ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("1"),
						},
					},
				},
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 1},
				"B": {0},
			},
		},
		"fairness two queues": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 32),
				"B": testNSmallCpuJob(0, 32),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
			ExpectedResourcesByQueue: map[string]resourceLimits{
				"A": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("14")},
					map[string]resource.Quantity{"cpu": resource.MustParse("18")},
				),
				"B": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("14")},
					map[string]resource.Quantity{"cpu": resource.MustParse("18")},
				),
			},
		},
		"fairness three queues": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 32),
				"B": testNSmallCpuJob(0, 32),
				"C": testNSmallCpuJob(0, 32),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
				"B": 1,
				"C": 1,
			},
			ExpectedResourcesByQueue: map[string]resourceLimits{
				"A": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("8")},
					map[string]resource.Quantity{"cpu": resource.MustParse("12")},
				),
				"B": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("8")},
					map[string]resource.Quantity{"cpu": resource.MustParse("12")},
				),
				"C": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("8")},
					map[string]resource.Quantity{"cpu": resource.MustParse("12")},
				),
			},
		},
		"weighted fairness two queues": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 32),
				"B": testNSmallCpuJob(0, 32),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 2,
				"B": 1,
			},
			ExpectedResourcesByQueue: map[string]resourceLimits{
				"A": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("19")},
					map[string]resource.Quantity{"cpu": resource.MustParse("23")},
				),
				"B": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("9")},
					map[string]resource.Quantity{"cpu": resource.MustParse("13")},
				),
			},
		},
		// // TODO: Ths test fails. Investigate how combining resources, resource scarcity, and our custom asApproximateFloat function are supposed to work.
		// "fairness two queues with initial usage": {
		// 	SchedulingConfig: testSchedulingConfig(),
		// 	Nodes:            testNCpuNode(1, testPriorities),
		// 	ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
		// 		"A": testNSmallCpuJob(0, 32),
		// 		"B": testNSmallCpuJob(0, 32),
		// 	},
		// 	PriorityFactorsByQueue: map[string]float64{
		// 		"A": 1,
		// 		"B": 1,
		// 	},
		// 	InitialUsageByQueue: map[string]schedulerobjects.QuantityByPriorityAndResourceType{
		// 		"A": {
		// 			0: schedulerobjects.ResourceList{
		// 				Resources: map[string]resource.Quantity{
		// 					"cpu": resource.MustParse("100"),
		// 				},
		// 			},
		// 		},
		// 	},
		// 	ExpectedResourcesByQueue: map[string]resourceLimits{
		// 		"A": newResourceLimits(
		// 			map[string]resource.Quantity{"cpu": resource.MustParse("4")},
		// 			map[string]resource.Quantity{"cpu": resource.MustParse("8")},
		// 		),
		// 		"B": newResourceLimits(
		// 			map[string]resource.Quantity{"cpu": resource.MustParse("24")},
		// 			map[string]resource.Quantity{"cpu": resource.MustParse("28")},
		// 		),
		// 	},
		// },
		"node with no available capacity": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes: withUsedResources(
				0,
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				testNCpuNode(1, testPriorities),
			),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 1),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": nil,
			},
		},
		"node with some available capacity": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes: withUsedResources(
				0,
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("31"),
					},
				},
				testNCpuNode(1, testPriorities),
			),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 2),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"preempt used resources of lower-priority jobs": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes: withUsedResources(
				0,
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				testNCpuNode(1, testPriorities),
			),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNLargeCpuJob(1, 1),
			},
			PriorityFactorsByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
	}
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
			scheduler.Rand = util.NewThreadsafeRand(42)

			jobs, err := scheduler.Schedule(context.Background(), tc.InitialUsageByQueue)
			if !assert.NoError(t, err) {
				return
			}

			// Check that the right jobs got scheduled.
			if tc.ExpectedIndicesByQueue != nil {
				actualScheduledJobsByQueue := jobIdsByQueueFromJobs(jobs)
				for queue, expected := range expectedByQueue {
					actual, ok := actualScheduledJobsByQueue[queue]
					if !ok {
						// Since expected is []string{} if we expect no jobs to be scheduled.
						actual = make([]string, 0)
					}
					assert.Equal(t, expected, actual, "queue %s", queue)
				}
			}

			// Check that each queue was allocated the right amount of resources.
			if tc.ExpectedResourcesByQueue != nil {
				actualUsageByQueue := usageByQueue(jobs)
				for queue, usage := range actualUsageByQueue {
					fmt.Printf("%s usage: %v\n", queue, usage)
					assertResourceLimitsSatisfied(t, tc.ExpectedResourcesByQueue[queue], usage)
				}
			}
		})
	}
}

func intRange(a, b int) []int {
	rv := make([]int, b-a+1)
	for i := range rv {
		rv[i] = a + i
	}
	return rv
}

func apiJobsFromPodReqs(queue string, reqs []*schedulerobjects.PodRequirements) []*api.Job {
	rv := make([]*api.Job, len(reqs))
	for i, req := range reqs {
		rv[i] = apiJobFromPodSpec(queue, podSpecFromPodRequirements(req))
	}
	return rv
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
