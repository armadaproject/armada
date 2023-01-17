package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

const (
	testGangIdAnnotation          = "armada.io/gangId"
	testGangCardinalityAnnotation = "armada.io/gangCardinality"
)

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

func TestQueueSelectionWeights(t *testing.T) {
	tests := map[string]struct {
		PriorityFactorByQueue         map[string]float64
		AggregateResourceUsageByQueue map[string]schedulerobjects.ResourceList
		ResourceScarcity              map[string]float64
		ExpectedByQueue               map[string]float64
	}{
		"one queues": {
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ResourceScarcity: map[string]float64{"cpu": 1},
			ExpectedByQueue: map[string]float64{
				"A": 1.0,
			},
		},
		"two queues with equal factors no usage": {
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
			ResourceScarcity: map[string]float64{"cpu": 1},
			ExpectedByQueue: map[string]float64{
				"A": 1.0 / 2.0,
				"B": 1.0 / 2.0,
			},
		},
		"three queues with equal factors no usage": {
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
				"C": 1,
			},
			ResourceScarcity: map[string]float64{"cpu": 1},
			ExpectedByQueue: map[string]float64{
				"A": 1.0 / 3.0,
				"B": 1.0 / 3.0,
				"C": 1.0 / 3.0,
			},
		},
		"two queues with unequal factors no usage": {
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 2,
			},
			ResourceScarcity: map[string]float64{"cpu": 1},
			ExpectedByQueue: map[string]float64{
				"A": 2.0 / 3.0,
				"B": 1.0 / 3.0,
			},
		},
		"two queues with one far above its share": {
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
			AggregateResourceUsageByQueue: map[string]schedulerobjects.ResourceList{
				"B": {
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("100000"),
					},
				},
			},
			ResourceScarcity: map[string]float64{"cpu": 1},
			ExpectedByQueue: map[string]float64{
				"A": 1.0,
				"B": 0.0,
			},
		},
		"two queues using exactly using their share": {
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 2,
			},
			AggregateResourceUsageByQueue: map[string]schedulerobjects.ResourceList{
				"A": {
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("2"),
					},
				},
				"B": {
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("1"),
					},
				},
			},
			ResourceScarcity: map[string]float64{"cpu": 1},
			ExpectedByQueue: map[string]float64{
				"A": 1.0 / 2.0,
				"B": 1.0 / 2.0,
			},
		},
		"two queues with no usage": {
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
			AggregateResourceUsageByQueue: make(map[string]schedulerobjects.ResourceList),
			ResourceScarcity:              map[string]float64{"cpu": 1},
			ExpectedByQueue: map[string]float64{
				"A": 1.0 / 2.0,
				"B": 1.0 / 2.0,
			},
		},
		"two queues with unequal factors and no usage": {
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 2,
			},
			AggregateResourceUsageByQueue: make(map[string]schedulerobjects.ResourceList),
			ResourceScarcity:              map[string]float64{"cpu": 1},
			ExpectedByQueue: map[string]float64{
				"A": 2.0 / 3.0,
				"B": 1.0 / 3.0,
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			actualByQueue := queueSelectionWeights(
				tc.PriorityFactorByQueue,
				tc.AggregateResourceUsageByQueue,
				tc.ResourceScarcity,
			)
			if !assert.Equal(t, len(tc.ExpectedByQueue), len(actualByQueue)) {
				return
			}
			for queue, actual := range actualByQueue {
				expected, ok := tc.ExpectedByQueue[queue]
				if !assert.True(t, ok) {
					continue
				}
				assert.InDelta(t, expected, actual, 1e-2)
			}
		})
	}
}

func TestPickQueueRandomly(t *testing.T) {
	weights := map[string]float64{
		"A": 2,
		"B": 1,
	}
	weightsSum := 3.0

	n := 10000
	results := make(map[string]float64)
	for i := 0; i < n; i++ {
		v, _ := pickQueueRandomly(weights, util.NewThreadsafeRand(int64(i)))
		results[v]++
	}
	for queue, weight := range weights {
		expected := weight / weightsSum
		actual := results[queue] / float64(n)
		assert.InDelta(t, expected, actual, 0.01)
	}
}

func TestQueueCandidateGangIterator(t *testing.T) {
	tests := map[string]struct {
		Reqs                   []*schedulerobjects.PodRequirements
		InitialUsageByPriority schedulerobjects.QuantityByPriorityAndResourceType
		SchedulingConstraints  SchedulingConstraints
		// If true, jobs are actually leased.
		LeaseJobs bool
		// Indices of the reqs expected to be returned.
		ExpectedIndices []int
	}{
		"all jobs schedulable": {
			Reqs:            testNSmallCpuJob(0, 3),
			ExpectedIndices: []int{0, 1, 2},
		},
		"minimum job size below limit": {
			Reqs: append(testNSmallCpuJob(0, 3), testNLargeCpuJob(0, 2)...),
			SchedulingConstraints: SchedulingConstraints{
				MinimumJobSize: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("31"),
					},
				},
			},
			ExpectedIndices: []int{3, 4},
		},
		"minimum job size at limit": {
			Reqs: append(testNSmallCpuJob(0, 3), testNLargeCpuJob(0, 2)...),
			SchedulingConstraints: SchedulingConstraints{
				MinimumJobSize: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
			},
			ExpectedIndices: []int{3, 4},
		},
		"MaximalResourceFractionToSchedulePerQueue": {
			Reqs: testNSmallCpuJob(0, 3),
			SchedulingConstraints: SchedulingConstraints{
				TotalResources: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				MaximalResourceFractionToSchedulePerQueue: common.ComputeResourcesFloat{
					"cpu": 2.0 / 32.0,
				},
			},
			LeaseJobs:       true,
			ExpectedIndices: []int{0, 1},
		},
		"MaximalResourceFractionPerQueue": {
			Reqs: testNSmallCpuJob(0, 3),
			SchedulingConstraints: SchedulingConstraints{
				TotalResources: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				MaximalResourceFractionPerQueue: common.ComputeResourcesFloat{
					"cpu": 2.0 / 32.0,
				},
			},
			LeaseJobs:       true,
			ExpectedIndices: []int{0, 1},
		},
		"MaximalResourceFractionPerQueue with initial usage": {
			Reqs: testNSmallCpuJob(0, 3),
			SchedulingConstraints: SchedulingConstraints{
				TotalResources: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				MaximalResourceFractionPerQueue: common.ComputeResourcesFloat{
					"cpu": 2.0 / 32.0,
				},
			},
			InitialUsageByPriority: schedulerobjects.QuantityByPriorityAndResourceType{
				0: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("1"),
					},
				},
			},
			LeaseJobs:       true,
			ExpectedIndices: []int{0},
		},
		"MaxConsecutiveUnschedulableJobs": {
			Reqs: append(append(testNSmallCpuJob(0, 1), testNGPUJob(0, 3)...), testNSmallCpuJob(0, 1)...),
			SchedulingConstraints: SchedulingConstraints{
				TotalResources: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
						"gpu": resource.MustParse("1"),
					},
				},
				MaximalResourceFractionPerQueue: common.ComputeResourcesFloat{
					"gpu": 0,
				},
				MaxConsecutiveUnschedulableJobs: 3,
			},
			ExpectedIndices: []int{0},
		},
		"MaximalCumulativeResourceFractionPerQueueAndPriority": {
			Reqs: append(append(testNSmallCpuJob(9, 11), testNSmallCpuJob(7, 11)...), testNSmallCpuJob(3, 11)...),
			SchedulingConstraints: SchedulingConstraints{
				Priorities: []int32{3, 7, 9},
				TotalResources: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				MaximalCumulativeResourceFractionPerQueueAndPriority: map[int32]map[string]float64{
					3: {"cpu": 30.0 / 32.0},
					7: {"cpu": 20.0 / 32.0},
					9: {"cpu": 10.0 / 32.0},
				},
			},
			LeaseJobs:       true,
			ExpectedIndices: append(append(intRange(0, 9), intRange(11, 20)...), intRange(22, 31)...),
		},
		"MaximalCumulativeResourceFractionPerQueueAndPriority with initial usage": {
			Reqs: append(append(testNSmallCpuJob(9, 11), testNSmallCpuJob(7, 11)...), testNSmallCpuJob(3, 11)...),
			SchedulingConstraints: SchedulingConstraints{
				Priorities: []int32{3, 7, 9},
				TotalResources: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				MaximalCumulativeResourceFractionPerQueueAndPriority: map[int32]map[string]float64{
					3: {"cpu": 30.0 / 32.0},
					7: {"cpu": 20.0 / 32.0},
					9: {"cpu": 10.0 / 32.0},
				},
			},
			InitialUsageByPriority: schedulerobjects.QuantityByPriorityAndResourceType{
				3: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
				7: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("2")}},
				9: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("3")}},
			},
			LeaseJobs:       true,
			ExpectedIndices: append(append(intRange(0, 6), intRange(11, 18)...), intRange(22, 30)...),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			repo := newMockJobRepository()
			jobs := make([]*api.Job, len(tc.Reqs))
			indexByJobId := make(map[string]int)
			for i, req := range tc.Reqs {
				// Queue name doesn't matter.
				jobs[i] = apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
				repo.Enqueue(jobs[i])
				indexByJobId[jobs[i].Id] = i
			}

			expected := make([]*api.Job, len(tc.ExpectedIndices))
			for i, j := range tc.ExpectedIndices {
				expected[i] = jobs[j]
			}

			ctx := context.Background()
			queuedJobsIterator, err := NewQueuedJobsIterator(ctx, "A", repo)
			if !assert.NoError(t, err) {
				return
			}
			queuedGangIterator := NewQueuedGangIterator[*api.Job](
				ctx,
				queuedJobsIterator,
				testGangIdAnnotation,
				testGangCardinalityAnnotation,
			)
			it := &QueueCandidateGangIterator[*api.Job]{
				ctx:                        ctx,
				SchedulingConstraints:      tc.SchedulingConstraints,
				QueueSchedulingRoundReport: NewQueueSchedulingRoundReport[*api.Job](0, tc.InitialUsageByPriority),
				queuedGangIterator:         queuedGangIterator,
			}

			actual := make([]*api.Job, 0)
			actualIndices := make([]int, 0)
			for reports, err := it.Next(); reports != nil; reports, err = it.Next() {
				if !assert.NoError(t, err) {
					return
				}
				for _, report := range reports {
					if tc.LeaseJobs {
						it.QueueSchedulingRoundReport.AddJobSchedulingReport(report)
					}
					actual = append(actual, report.Job)
					actualIndices = append(actualIndices, indexByJobId[report.Job.GetId()])
				}
			}
			assert.Equal(t, tc.ExpectedIndices, actualIndices) // Redundant, but useful to debug tests.
			assert.Equal(t, expected, actual, "")
		})
	}
}

func testSchedulingConfig() configuration.SchedulingConfig {
	priorityClasses := make(map[string]configuration.PriorityClass)
	for _, priority := range testPriorityClasses {
		priorityClasses[fmt.Sprintf("%d", priority.Priority)] = priority
	}
	return configuration.SchedulingConfig{
		ResourceScarcity: map[string]float64{"cpu": 1, "memory": 0},
		Preemption: configuration.PreemptionConfig{
			PriorityClasses: priorityClasses,
		},
		IndexedResources:          []string{"cpu", "memory"},
		GangIdAnnotation:          testGangIdAnnotation,
		GangCardinalityAnnotation: testGangCardinalityAnnotation,
		ExecutorTimeout:           15 * time.Minute,
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

func withPerPriorityLimits(limits map[int32]map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	for k, v := range config.Preemption.PriorityClasses {
		config.Preemption.PriorityClasses[k] = configuration.PriorityClass{
			Priority:                        v.Priority,
			MaximalResourceFractionPerQueue: limits[v.Priority],
		}
	}
	return config
}

func withPerQueueRoundLimits(limits map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximalResourceFractionToSchedulePerQueue = limits
	return config
}

func withMaxJobsToSchedule(n uint, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximumJobsToSchedule = n
	return config
}

func withIndexedTaints(indexedTaints []string, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.IndexedTaints = append(config.IndexedTaints, indexedTaints...)
	return config
}

func withIndexedNodeLabels(indexedNodeLabels []string, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.IndexedNodeLabels = append(config.IndexedNodeLabels, indexedNodeLabels...)
	return config
}

func withUsedResources(p int32, rs schedulerobjects.ResourceList, nodes []*schedulerobjects.Node) []*schedulerobjects.Node {
	for _, node := range nodes {
		schedulerobjects.AllocatableByPriorityAndResourceType(node.AllocatableByPriorityAndResource).MarkAllocated(p, rs)
	}
	return nodes
}

func withLabels(labels map[string]string, nodes []*schedulerobjects.Node) []*schedulerobjects.Node {
	for _, node := range nodes {
		if node.Labels == nil {
			node.Labels = maps.Clone(labels)
		} else {
			maps.Copy(node.Labels, labels)
		}
	}
	return nodes
}

func withNodeSelector(selector map[string]string, reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	for _, req := range reqs {
		req.NodeSelector = maps.Clone(selector)
	}
	return reqs
}

func withGangAnnotations(reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	gangId := uuid.NewString()
	gangCardinality := fmt.Sprintf("%d", len(reqs))
	return withAnnotations(
		map[string]string{testGangIdAnnotation: gangId, testGangCardinalityAnnotation: gangCardinality},
		reqs,
	)
}

func withAnnotations(annotations map[string]string, reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	for _, req := range reqs {
		if req.Annotations == nil {
			req.Annotations = make(map[string]string)
		}
		maps.Copy(req.Annotations, annotations)
	}
	return reqs
}

func TestSchedule(t *testing.T) {
	tests := map[string]struct {
		SchedulingConfig configuration.SchedulingConfig
		// Nodes to be considered by the scheduler.
		Nodes []*schedulerobjects.Node
		// Map from queue name to pod requirements for that queue.
		ReqsByQueue map[string][]*schedulerobjects.PodRequirements
		// Map from queue to the priority factor associated with that queue.
		PriorityFactorByQueue map[string]float64
		// Initial resource usage for all queues.
		InitialUsageByQueue map[string]schedulerobjects.QuantityByPriorityAndResourceType
		// Total resources across all clusters.
		// If empty, it is computed as the total resources across the provided nodes.
		TotalResources schedulerobjects.ResourceList
		// Minimum job size.
		MinimumJobSize map[string]resource.Quantity
		// Skip checking if reports were generated.
		// Needed for tests where not all jobs are considered.
		DoNotCheckReports bool
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
			PriorityFactorByQueue: map[string]float64{
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
			PriorityFactorByQueue: map[string]float64{
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
			PriorityFactorByQueue: map[string]float64{
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
			PriorityFactorByQueue: map[string]float64{
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
			PriorityFactorByQueue: map[string]float64{
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
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"unschedulable jobs do not block schedulable jobs": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(append(testNSmallCpuJob(0, 1), testNLargeCpuJob(0, 10)...), testNSmallCpuJob(0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
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
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			DoNotCheckReports: true,
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
			PriorityFactorByQueue: map[string]float64{
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
			PriorityFactorByQueue: map[string]float64{
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
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 1},
				"B": {0, 1},
			},
		},
		"overall per-queue limits with large memory amount": {
			SchedulingConfig: withPerQueueLimits(
				map[string]float64{
					"cpu":    2.0 / 162975640.0,
					"memory": 0.1,
				},
				testSchedulingConfig()),
			Nodes: testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 5),
				"B": testNSmallCpuJob(0, 5),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
			TotalResources: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"memory": resource.MustParse("5188205838208Ki"),
					"cpu":    resource.MustParse("162975640"),
				},
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
			PriorityFactorByQueue: map[string]float64{
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
		"per priority per-queue limits": {
			SchedulingConfig: withPerPriorityLimits(
				map[int32]map[string]float64{
					0: {"cpu": 1.0},
					1: {"cpu": 0.5},
					2: {"cpu": 0.25},
					3: {"cpu": 0.1},
				}, testSchedulingConfig()),
			Nodes: testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob(3, 5), testNSmallCpuJob(0, 5)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			InitialUsageByQueue: map[string]schedulerobjects.QuantityByPriorityAndResourceType{},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 1, 2, 5, 6, 7, 8, 9},
			},
		},
		"per priority per queue limits equal limits": {
			SchedulingConfig: withPerPriorityLimits(
				map[int32]map[string]float64{
					0: {"cpu": 0.9}, // 28 cpu
					1: {"cpu": 0.9},
				}, testSchedulingConfig()),
			Nodes: testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob(0, 5), testNSmallCpuJob(0, 5)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			InitialUsageByQueue: map[string]schedulerobjects.QuantityByPriorityAndResourceType{
				"A": {
					0: schedulerobjects.ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("13"),
						},
					},
					1: schedulerobjects.ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("14"),
						},
					},
				},
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"limit hit at higher priority doesn't block jobs at lower priority": {
			SchedulingConfig: withPerPriorityLimits(
				map[int32]map[string]float64{
					0: {"cpu": 0.9}, // 28 cpu
					1: {"cpu": 0.5}, // 14 cpu
				}, testSchedulingConfig()),
			Nodes: testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob(1, 1), testNSmallCpuJob(0, 5)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			InitialUsageByQueue: map[string]schedulerobjects.QuantityByPriorityAndResourceType{
				"A": {
					0: schedulerobjects.ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("7"), // out of 28
						},
					},
					1: schedulerobjects.ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("20"), // out of 14, i.e., over the limit
						},
					},
				},
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {1},
			},
		},
		"fairness two queues": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 32),
				"B": testNSmallCpuJob(0, 32),
			},
			PriorityFactorByQueue: map[string]float64{
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
			PriorityFactorByQueue: map[string]float64{
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
			Nodes:            testNCpuNode(30, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 960),
				"B": testNSmallCpuJob(0, 960),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 2,
			},
			ExpectedResourcesByQueue: map[string]resourceLimits{
				"A": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("620")},
					map[string]resource.Quantity{"cpu": resource.MustParse("660")},
				),
				"B": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("300")},
					map[string]resource.Quantity{"cpu": resource.MustParse("340")},
				),
			},
		},
		"weighted fairness three queues": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(30, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 960),
				"B": testNSmallCpuJob(0, 960),
				"C": testNSmallCpuJob(0, 960),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 2,
				"C": 10,
			},
			ExpectedResourcesByQueue: map[string]resourceLimits{
				"A": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("580")},
					map[string]resource.Quantity{"cpu": resource.MustParse("620")},
				),
				"B": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("280")},
					map[string]resource.Quantity{"cpu": resource.MustParse("320")},
				),
				"C": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("40")},
					map[string]resource.Quantity{"cpu": resource.MustParse("80")},
				),
			},
		},
		"fairness two queues with initial usage": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob(0, 32),
				"B": testNSmallCpuJob(0, 32),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
			InitialUsageByQueue: map[string]schedulerobjects.QuantityByPriorityAndResourceType{
				"A": {
					0: schedulerobjects.ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("100"),
						},
					},
				},
			},
			ExpectedResourcesByQueue: map[string]resourceLimits{
				"A": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("0")},
					map[string]resource.Quantity{"cpu": resource.MustParse("4")},
				),
				"B": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("28")},
					map[string]resource.Quantity{"cpu": resource.MustParse("32")},
				),
			},
		},
		"Node with no available capacity": {
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
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": nil,
			},
		},
		"Node with some available capacity": {
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
			PriorityFactorByQueue: map[string]float64{
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
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"respect taints": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNTaintedCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob(0, 1), testNLargeCpuJob(0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {1},
			},
		},
		"minimum job size": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob(0, 1), testNLargeCpuJob(0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			MinimumJobSize: map[string]resource.Quantity{
				"cpu": resource.MustParse("2"),
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {1},
			},
		},
		"minimum job size gpu": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNGpuNode(2, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(append(testNSmallCpuJob(0, 1), testNLargeCpuJob(0, 1)...), testNGPUJob(0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			MinimumJobSize: map[string]resource.Quantity{
				"gpu": resource.MustParse("1"),
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {2},
			},
		},
		"minimum job size two gpu": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNGpuNode(2, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(append(testNSmallCpuJob(0, 1), testNLargeCpuJob(0, 1)...), testNGPUJob(0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			MinimumJobSize: map[string]resource.Quantity{
				"gpu": resource.MustParse("2"),
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {},
			},
		},
		"taints and tolerations": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNTaintedCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob(0, 1), testNLargeCpuJob(0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {1},
			},
		},
		"Node selector": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes: append(
				testNCpuNode(1, testPriorities),
				withLabels(map[string]string{"foo": "foo"}, testNCpuNode(1, testPriorities))...,
			),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": withNodeSelector(map[string]string{"foo": "foo"}, testNLargeCpuJob(0, 2)),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"taints and tolerations (indexed)": {
			SchedulingConfig: withIndexedTaints([]string{"largeJobsOnly"}, testSchedulingConfig()),
			Nodes:            testNTaintedCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob(0, 1), testNLargeCpuJob(0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {1},
			},
		},
		"Node selector (indexed)": {
			SchedulingConfig: withIndexedNodeLabels([]string{"foo"}, testSchedulingConfig()),
			Nodes: append(
				testNCpuNode(1, testPriorities),
				withLabels(map[string]string{"foo": "foo"}, testNCpuNode(1, testPriorities))...,
			),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": withNodeSelector(map[string]string{"foo": "foo"}, testNLargeCpuJob(0, 2)),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"gang scheduling success": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(2, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": withGangAnnotations(testNLargeCpuJob(0, 2)),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 1},
			},
		},
		"gang scheduling failure": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(2, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": withGangAnnotations(testNLargeCpuJob(0, 3)),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {},
			},
		},
		"gang aggregated resource accounting": {
			SchedulingConfig: withPerQueueLimits(
				map[string]float64{
					"cpu": 2.0 / 32.0,
				},
				testSchedulingConfig(),
			),
			Nodes: testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(append(
					withAnnotations(map[string]string{testGangIdAnnotation: "my-gang", testGangCardinalityAnnotation: "2"}, testNSmallCpuJob(0, 1)),
					testNSmallCpuJob(0, 1)...),
					withAnnotations(map[string]string{testGangIdAnnotation: "my-gang", testGangCardinalityAnnotation: "2"}, testNSmallCpuJob(0, 1))...,
				),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {1},
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

			nodeDb, err := createNodeDb(tc.Nodes)
			if !assert.NoError(t, err) {
				return
			}

			// If not provided, set total resources equal to the aggregate over tc.Nodes.
			if tc.TotalResources.Resources == nil {
				tc.TotalResources = nodeDb.totalResources.DeepCopy()
			}

			constraints := SchedulingConstraintsFromSchedulingConfig(
				"executor",
				"pool",
				schedulerobjects.ResourceList{Resources: tc.MinimumJobSize},
				tc.SchedulingConfig,
				tc.TotalResources,
			)
			sched, err := NewLegacyScheduler[*api.Job](
				context.Background(),
				*constraints,
				tc.SchedulingConfig,
				nodeDb,
				jobRepository,
				tc.PriorityFactorByQueue,
				tc.InitialUsageByQueue,
			)
			if !assert.NoError(t, err) {
				return
			}
			sched.CandidateGangIterator.rand = util.NewThreadsafeRand(42) // Reproducible tests.

			jobs, err := sched.Schedule()
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
					assertResourceLimitsSatisfied(t, tc.ExpectedResourcesByQueue[queue], usage)
				}
			}

			// Check that a scheduling round report was created.
			if !assert.NotNil(t, sched.SchedulingRoundReport) {
				return
			}

			// Check that scheduling reports were generated.
			// TODO: Check that reports correctly indicate success/not.
			if !tc.DoNotCheckReports {
				schedulingRoundReport := sched.SchedulingRoundReport

				// Check that started and finished it set.
				assert.NotEqual(t, time.Time{}, schedulingRoundReport.Started)
				assert.NotEqual(t, time.Time{}, schedulingRoundReport.Finished)

				// Check that initial usage is correct.
				assert.Equal(t, len(tc.PriorityFactorByQueue), len(schedulingRoundReport.QueueSchedulingRoundReports))
				for queue, usage := range tc.InitialUsageByQueue {
					queueSchedulingRoundReport := sched.SchedulingRoundReport.QueueSchedulingRoundReports[queue]
					if assert.NotNil(t, queueSchedulingRoundReport) {
						assert.True(t, usage.Equal(queueSchedulingRoundReport.InitialResourcesByPriority))
					}
				}

				// Check that scheduling round report scheduled resources is set correctly.
				for queue, expected := range usageByQueueAndPriority(jobs, tc.SchedulingConfig.Preemption.PriorityClasses) {
					queueSchedulingRoundReport, ok := schedulingRoundReport.QueueSchedulingRoundReports[queue]
					if !assert.NotNil(t, queueSchedulingRoundReport) {
						continue
					}
					if !assert.True(t, ok) {
						continue
					}
					actual := queueSchedulingRoundReport.ScheduledResourcesByPriority
					assert.True(t, expected.Equal(actual))
				}

				// Check that the scheduling round report contains reports for all queues and jobs.
				assert.Equal(
					t,
					len(tc.PriorityFactorByQueue),
					len(sched.SchedulingRoundReport.QueueSchedulingRoundReports),
				)
				leasedJobIds := make(map[uuid.UUID]interface{})
				for _, job := range jobs {
					jobId, err := uuidFromUlidString(job.GetId())
					if !assert.NoError(t, err) {
						return
					}
					leasedJobIds[jobId] = true
				}
				for queue, jobs := range jobRepository.jobsByQueue {
					queueSchedulingRoundReport, ok := schedulingRoundReport.QueueSchedulingRoundReports[queue]
					if !assert.NotNil(t, queueSchedulingRoundReport) {
						continue
					}
					if !assert.True(t, ok) {
						continue
					}

					for _, job := range jobs {
						jobId, err := uuidFromUlidString(job.Id)
						if !assert.NoError(t, err) {
							return
						}

						var jobReports map[uuid.UUID]*JobSchedulingReport[*api.Job]
						if _, ok := leasedJobIds[jobId]; ok {
							jobReports = queueSchedulingRoundReport.SuccessfulJobSchedulingReports
						} else {
							jobReports = queueSchedulingRoundReport.UnsuccessfulJobSchedulingReports
						}
						if !assert.NotNil(t, jobReports) {
							continue
						}

						jobReport, ok := jobReports[jobId]
						if !assert.NotNil(t, jobReport) {
							continue
						}
						if !assert.True(t, ok) {
							continue
						}
					}
				}

				// Check that total resources is correct.
				assert.True(t, schedulingRoundReport.TotalResources.Equal(tc.TotalResources))

				// Check that we were given a termination reason.
				assert.NotEmpty(t, schedulingRoundReport.TerminationReason)
			}
		})
	}
}

func Test_exceedsPerPriorityResourceLimits(t *testing.T) {
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
		rv[i].Annotations = maps.Clone(req.Annotations)
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

func jobIdsByQueueFromJobs(jobs []*api.Job) map[string][]string {
	rv := make(map[string][]string)
	for _, job := range jobs {
		rv[job.Queue] = append(rv[job.Queue], job.Id)
	}
	return rv
}

func usageByQueue(jobs []*api.Job) map[string]schedulerobjects.ResourceList {
	// TODO: Could be built on top of usageByQueueAndPriority.
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

func usageByQueueAndPriority(jobs []*api.Job, priorityByPriorityClassName map[string]configuration.PriorityClass) map[string]schedulerobjects.QuantityByPriorityAndResourceType {
	rv := make(map[string]schedulerobjects.QuantityByPriorityAndResourceType)
	for _, job := range jobs {
		m, ok := rv[job.Queue]
		if !ok {
			m = make(schedulerobjects.QuantityByPriorityAndResourceType)
			rv[job.Queue] = m
		}
		priority, _ := PriorityFromJob(job, priorityByPriorityClassName)
		rl, ok := m[priority]
		if !ok {
			rl.Resources = make(map[string]resource.Quantity)
		}
		for t, q := range common.TotalJobResourceRequest(job) {
			quantity := rl.Resources[t]
			quantity.Add(q)
			rl.Resources[t] = quantity
		}
		m[priority] = rl
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
	jobsByQueue map[string][]*api.Job
	jobsById    map[string]*api.Job
	// Ids of all jobs hat were leased to an executor.
	leasedJobs          map[string]bool
	getQueueJobIdsDelay time.Duration
}

func newMockJobRepository() *mockJobRepository {
	return &mockJobRepository{
		jobsByQueue: make(map[string][]*api.Job),
		jobsById:    make(map[string]*api.Job),
		leasedJobs:  make(map[string]bool),
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

func (repo *mockJobRepository) GetJobIterator(ctx context.Context, queue string) (JobIterator[*api.Job], error) {
	return NewQueuedJobsIterator(ctx, queue, repo)
}

func (repo *mockJobRepository) GetQueueJobIds(queue string) ([]string, error) {
	time.Sleep(repo.getQueueJobIdsDelay)
	if jobs, ok := repo.jobsByQueue[queue]; ok {
		rv := make([]string, 0, len(jobs))
		for _, job := range jobs {
			if !repo.leasedJobs[job.Id] {
				rv = append(rv, job.Id)
			}
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
	successfullyLeasedJobs := make([]*api.Job, 0, len(jobs))
	for _, job := range jobs {
		if !repo.leasedJobs[job.Id] {
			successfullyLeasedJobs = append(successfullyLeasedJobs, job)
			repo.leasedJobs[job.Id] = true
		}
	}
	return successfullyLeasedJobs, nil
}

func benchmarkQuantityComparison(b *testing.B, q1, q2 resource.Quantity) {
	for i := 0; i < b.N; i++ {
		q1.Cmp(q2)
	}
}

func BenchmarkQuantityComparison(b *testing.B) {
	benchmarkQuantityComparison(b, resource.MustParse("1"), resource.MustParse("2"))
}

func BenchmarkIntComparison(b *testing.B) {
	result := 0
	v1 := 1
	v2 := 2
	for i := 0; i < b.N; i++ {
		if v1 == v2 {
			result += 1
		}
	}
}
