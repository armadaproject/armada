package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestEvictOversubscribed(t *testing.T) {
	nodes := testfixtures.TestNCpuNode(1, testfixtures.TestPriorities)
	node := nodes[0]
	var err error
	jobs := append(
		testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 20),
		testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass1, 20)...,
	)
	reqs := PodRequirementsFromLegacySchedulerJobs(jobs, testfixtures.TestPriorityClasses)
	for _, req := range reqs {
		node, err = nodedb.BindPodToNode(req, node)
		require.NoError(t, err)
	}
	nodes[0] = node

	jobRepo := NewInMemoryJobRepository(testfixtures.TestPriorityClasses)
	for _, job := range jobs {
		jobRepo.Enqueue(job)
	}
	evictor := NewOversubscribedEvictor(
		jobRepo,
		testfixtures.TestPriorityClasses,
		1,
		nil,
	)
	it := NewInMemoryNodeIterator(nodes)
	result, err := evictor.Evict(context.Background(), it)
	require.NoError(t, err)

	prioritiesByName := configuration.PriorityByPriorityClassName(testfixtures.TestPriorityClasses)
	priorities := maps.Values(prioritiesByName)
	slices.Sort(priorities)
	for nodeId, node := range result.AffectedNodesById {
		for _, p := range priorities {
			for resourceType, q := range node.AllocatableByPriorityAndResource[p].Resources {
				assert.NotEqual(t, -1, q.Cmp(resource.Quantity{}), "resource %s oversubscribed by %s on node %s", resourceType, q.String(), nodeId)
			}
		}
	}
}

type InMemoryNodeIterator struct {
	i     int
	nodes []*schedulerobjects.Node
}

func NewInMemoryNodeIterator(nodes []*schedulerobjects.Node) *InMemoryNodeIterator {
	return &InMemoryNodeIterator{
		nodes: slices.Clone(nodes),
	}
}

func (it *InMemoryNodeIterator) NextNode() *schedulerobjects.Node {
	if it.i >= len(it.nodes) {
		return nil
	}
	v := it.nodes[it.i]
	it.i++
	return v
}

func TestPreemptingQueueScheduler(t *testing.T) {
	type SchedulingRound struct {
		// Map from queue name to pod requirements for that queue.
		JobsByQueue map[string][]*jobdb.Job
		// For each queue, indices of jobs expected to be scheduled.
		ExpectedScheduledIndices map[string][]int
		// For each queue, indices of jobs expected to be preempted.
		// E.g., ExpectedPreemptedIndices["A"][0] is the indices of jobs declared for queue A in round 0.
		ExpectedPreemptedIndices map[string]map[int][]int
		// For each queue, indices of jobs to unbind before scheduling, to, simulate jobs terminating.
		// E.g., IndicesToUnbind["A"][0] is the indices of jobs declared for queue A in round 0.
		IndicesToUnbind map[string]map[int][]int
	}
	tests := map[string]struct {
		SchedulingConfig configuration.SchedulingConfig
		// Nodes to be considered by the scheduler.
		Nodes []*schedulerobjects.Node
		// Each item corresponds to a call to Reschedule().
		Rounds []SchedulingRound
		// Map from queue to the priority factor associated with that queue.
		PriorityFactorByQueue map[string]float64
		// Initial resource usage for all queues.
		// This value is used across all rounds,
		// i.e., we don't update it based on preempted/scheduled jobs.
		InitialAllocationByQueue map[string]schedulerobjects.QuantityByPriorityAndResourceType
		// Total resources across all clusters.
		// If empty, it is computed as the total resources across the provided nodes.
		TotalResources schedulerobjects.ResourceList
		// Minimum job size.
		MinimumJobSize map[string]resource.Quantity
	}{
		"balancing three queues": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 15),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(16, 31),
						},
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"C": testfixtures.NSmallCpuJob("C", testfixtures.PriorityClass0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"C": testfixtures.IntRange(0, 9),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(11, 15),
						},
						"B": {
							1: testfixtures.IntRange(11, 15),
						},
					},
				},
				{
					// The system should be in steady-state; nothing should be scheduled/preempted.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1),
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 1),
						"C": testfixtures.NSmallCpuJob("C", testfixtures.PriorityClass0, 1),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
				"C": 1,
			},
		},
		"balancing two queues weighted": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 20),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(11, 31),
						},
					},
				},
				{
					// The system should be in steady-state; nothing should be scheduled/preempted.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1),
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 1),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 2,
				"B": 1,
			},
		},
		"balancing two queues weighted with inactive queues": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 20),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(11, 31),
						},
					},
				},
				{
					// The system should be in steady-state; nothing should be scheduled/preempted.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1),
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 1),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 2,
				"B": 1,
				"C": 1,
				"D": 100,
			},
		},
		"reschedule onto same node": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 31),
					},
				},
				{}, // Empty round to make sure nothing changes.
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"reschedule onto same node reverse order": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{}, // Empty round to make sure nothing changes.
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"urgency-based preemption stability": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass2, 33),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass3, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 0),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(31, 31),
						},
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass2, 1),
					},
				},
				{}, // Empty round to make sure nothing changes.
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"avoid urgency-based preemptions when possible": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					// These should all be scheduled onto the second node with no preemptions necessary.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass1, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"preempt in order of priority": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass1, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					// This job should preempt the priority-0 jobs.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass2, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							1: testfixtures.IntRange(0, 0),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"avoid urgency-based preemptions when possible cross-queue": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(3, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass1, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					// These should all be scheduled onto the second node with no preemptions necessary.
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.NLargeCpuJob("B", testfixtures.PriorityClass0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 0),
					},
				},
				{
					// These should all be scheduled onto the second node with no preemptions necessary.
					JobsByQueue: map[string][]*jobdb.Job{
						"C": testfixtures.NLargeCpuJob("C", testfixtures.PriorityClass2, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"C": testfixtures.IntRange(0, 0),
					},
				},
				{
					// These should all be scheduled onto the second node with no preemptions necessary.
					JobsByQueue: map[string][]*jobdb.Job{
						"D": testfixtures.NLargeCpuJob("D", testfixtures.PriorityClass3, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"D": testfixtures.IntRange(0, 0),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"B": {
							1: testfixtures.IntRange(0, 0),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
				"C": 1,
				"D": 1,
			},
		},
		"gang preemption": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					// Fill half of node 1 and half of node 2.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 16),
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 16),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 15),
						"B": testfixtures.IntRange(0, 15),
					},
				},
				{
					// Schedule a gang filling the remaining space on both nodes.
					JobsByQueue: map[string][]*jobdb.Job{
						"C": testfixtures.WithGangAnnotationsJobs(testfixtures.NSmallCpuJob("C", testfixtures.PriorityClass0, 32)),
					},
					ExpectedScheduledIndices: map[string][]int{
						"C": testfixtures.IntRange(0, 31),
					},
				},
				{
					// Schedule jobs that requires preempting one job in the gang,
					// and assert that all jobs in the gang are preempted.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass1, 17),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 16),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"C": {
							1: testfixtures.IntRange(0, 31),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
				"C": 1,
			},
		},
		"gang preemption with partial gang": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					// Schedule a gang across two nodes.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.WithGangAnnotationsJobs(testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 2)),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 1),
					},
				},
				{
					// Unbind one of the jobs in the gang (simulating that job terminating)
					// and test that the remaining job isn't preempted.
					IndicesToUnbind: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(0, 0),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"gang preemption with NodeEvictionProbability 0": {
			SchedulingConfig: testfixtures.WithNodeEvictionProbabilityConfig(
				0.0, // To test the gang evictor, we need to disable stochastic eviction.
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					// Schedule a gang filling all of node 1 and part of node 2.
					// Make the jobs of node 1 priority 1,
					// to avoid them being urgency-preempted in the next round.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.WithGangAnnotationsJobs(
							append(testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass1, 32), testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1)...),
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 32),
					},
				},
				{
					// Schedule a that requires preempting one job in the gang,
					// and assert that all jobs in the gang are preempted.
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.NLargeCpuJob("B", testfixtures.PriorityClass1, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 0),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(0, 32),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"gang preemption avoid cascading preemptions": {
			SchedulingConfig: testfixtures.WithNodeEvictionProbabilityConfig(
				0.0, // To test the gang evictor, we need to disable stochastic eviction.
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.TestNCpuNode(3, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					// Schedule a gang spanning nodes 1 and 2.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.WithGangAnnotationsJobs(testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass1, 33)),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 32),
					},
				},
				{
					// Schedule a gang spanning nodes 2 and 3.
					// Make the one job landing on node 3 have priority 0, so it will be urgency-preempted next.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.WithGangAnnotationsJobs(
							append(testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass1, 31), testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1)...),
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{
					// Schedule a job that requires preempting the one job on node 3.
					// Assert that the entire second gang is preempted and that the first gang isn't.
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.NLargeCpuJob("B", testfixtures.PriorityClass1, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 0),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							1: testfixtures.IntRange(0, 31),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"rescheduled jobs don't count towards maxJobsToSchedule": {
			SchedulingConfig: testfixtures.WithMaxJobsToScheduleConfig(5, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 4),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 4),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"rescheduled jobs don't count towards maxLookbackPerQueue": {
			SchedulingConfig: testfixtures.WithMaxLookbackPerQueueConfig(5, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					// JobsByQueue: map[string][]*jobdb.Job{
					// 	"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 10),
					// },
					// ExpectedScheduledIndices: map[string][]int{
					// 	"A": testfixtures.IntRange(0, 4),
					// },
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 2),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 1),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 4),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"rescheduled jobs don't count towards MaximalResourceFractionToSchedulePerQueue": {
			SchedulingConfig: testfixtures.WithPerQueueRoundLimitsConfig(
				map[string]float64{
					"cpu": 5.0 / 32.0,
				},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 4),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 4),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"rescheduled jobs don't count towards MaximalClusterFractionToSchedule": {
			SchedulingConfig: testfixtures.WithRoundLimitsConfig(
				map[string]float64{
					"cpu": 5.0 / 32.0,
				},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 4),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 4),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"priority class preemption two classes": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass1, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(0, 0),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"priority class preemption cross-queue": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.NLargeCpuJob("B", testfixtures.PriorityClass1, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 0),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(0, 0),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"priority class preemption not scheduled": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": append(testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 1), testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass1, 1)...),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(1, 1),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"priority class preemption four classes": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": append(append(
							testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 10),
							testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass1, 10)...),
							testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass2, 10)...,
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 29),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass3, 24),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 23),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: append(testfixtures.IntRange(0, 19), testfixtures.IntRange(28, 29)...),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"per-priority class limits": {
			SchedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[int32]map[string]float64{
					0: {"cpu": 60.0 / 64.0},
					1: {"cpu": 20.0 / 64.0},
				},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": append(
							testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass1, 64),
							testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 64)...,
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": append(testfixtures.IntRange(0, 19), testfixtures.IntRange(64, 103)...),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"per-priority class limits multiple rounds": {
			SchedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[int32]map[string]float64{
					0: {"cpu": 30.0 / 32.0},
					1: {"cpu": 10.0 / 32.0},
				},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": append(
							testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass1, 5),
							testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 10)...,
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 14),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": append(
							testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass1, 32),
							testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32)...,
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": append(testfixtures.IntRange(0, 4), testfixtures.IntRange(32, 41)...),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": append(
							testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass1, 32),
							testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32)...,
						),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"MaximalClusterFractionToSchedule": {
			SchedulingConfig: testfixtures.WithRoundLimitsConfig(
				map[string]float64{"cpu": 0.25},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 15),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 64),
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 15),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 64),
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 7),
						"B": testfixtures.IntRange(0, 7),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 64),
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 7),
						"B": testfixtures.IntRange(0, 7),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"MaximalResourceFractionToSchedulePerQueue": {
			SchedulingConfig: testfixtures.WithPerQueueRoundLimitsConfig(
				map[string]float64{"cpu": 0.25},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 15),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 15),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 15),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 15),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 64),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"MaximalResourceFractionPerQueue": {
			SchedulingConfig: testfixtures.WithPerQueueLimitsConfig(
				map[string]float64{"cpu": 0.5},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 15),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"Queued jobs are not preempted cross queue": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass1, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 31),
					},
				},
				{}, // Empty round to make sure nothing changes.
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"Queued jobs are not preempted cross queue with some scheduled": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass1, 31),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
						"B": testfixtures.IntRange(0, 30),
					},
				},
				{}, // Empty round to make sure nothing changes.
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"Queued jobs are not preempted cross queue with non-preemptible jobs": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass3, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 31),
					},
				},
				{}, // Empty round to make sure nothing changes.
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"Queued jobs are not preempted cross queue multiple rounds": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass1, 16),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 15),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 16),
						"B": testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass1, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 15),
					},
				},
				{}, // Empty round to make sure nothing changes.
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := CreateNodeDb(tc.Nodes)
			require.NoError(t, err)

			// Repo. for storing jobs to be queued.
			// The Redis job repo. doesn't order by pc, so we disable pc ordering here too.
			repo := NewInMemoryJobRepository(testfixtures.TestPriorityClasses)
			repo.sortByPriorityClass = false

			// Accounting across scheduling rounds.
			roundByJobId := make(map[string]int)
			indexByJobId := make(map[string]int)
			allocatedByQueueAndPriority := armadamaps.DeepCopy(tc.InitialAllocationByQueue)
			nodeIdByJobId := make(map[string]string)
			var jobIdsByGangId map[string]map[string]bool
			var gangIdByJobId map[string]string

			// Run the scheduler.
			log := logrus.NewEntry(logrus.New())
			for i, round := range tc.Rounds {
				log = log.WithField("round", i)
				log.Infof("starting scheduling round %d", i)

				// Reset the queues between rounds.
				repo.jobsByQueue = make(map[string][]interfaces.LegacySchedulerJob)

				// Add jobs that should be queued in this round.
				legacySchedulerJobs := make([]interfaces.LegacySchedulerJob, 0)
				for queue, jobs := range round.JobsByQueue {
					for j, job := range jobs {
						require.Equal(t, queue, job.GetQueue())
						legacySchedulerJobs = append(legacySchedulerJobs, job)
						roundByJobId[job.GetId()] = i
						indexByJobId[job.GetId()] = j
					}
				}
				repo.EnqueueMany(legacySchedulerJobs)

				// Unbind jobs from nodes, to simulate those jobs terminating between rounds.
				for queue, reqIndicesByRoundIndex := range round.IndicesToUnbind {
					for roundIndex, reqIndices := range reqIndicesByRoundIndex {
						for _, reqIndex := range reqIndices {
							job := tc.Rounds[roundIndex].JobsByQueue[queue][reqIndex]
							req := PodRequirementFromLegacySchedulerJob(job, tc.SchedulingConfig.Preemption.PriorityClasses)
							nodeId := nodeIdByJobId[job.GetId()]
							node, err := nodeDb.GetNode(nodeId)
							require.NoError(t, err)
							node, err = nodedb.UnbindPodFromNode(req, node)
							require.NoError(t, err)
							err = nodeDb.Upsert(node)
							require.NoError(t, err)
							if gangId, ok := gangIdByJobId[job.GetId()]; ok {
								delete(gangIdByJobId, job.GetId())
								delete(jobIdsByGangId[gangId], job.GetId())
							}
						}
					}
				}

				// If not provided, set total resources equal to the aggregate over tc.Nodes.
				if tc.TotalResources.Resources == nil {
					tc.TotalResources = nodeDb.TotalResources()
				}

				sctx := schedulercontext.NewSchedulingContext(
					"executor",
					"pool",
					tc.SchedulingConfig.Preemption.PriorityClasses,
					tc.SchedulingConfig.Preemption.DefaultPriorityClass,
					tc.SchedulingConfig.ResourceScarcity,
					tc.PriorityFactorByQueue,
					tc.TotalResources,
					allocatedByQueueAndPriority,
				)
				constraints := schedulerconstraints.SchedulingConstraintsFromSchedulingConfig(
					schedulerobjects.ResourceList{Resources: tc.MinimumJobSize},
					tc.SchedulingConfig,
				)
				sch := NewPreemptingQueueScheduler(
					sctx,
					constraints,
					tc.SchedulingConfig.Preemption.NodeEvictionProbability,
					tc.SchedulingConfig.Preemption.NodeOversubscriptionEvictionProbability,
					repo,
					nodeDb,
					nodeIdByJobId,
					jobIdsByGangId,
					gangIdByJobId,
				)
				sch.EnableAssertions()
				result, err := sch.Schedule(ctxlogrus.ToContext(context.Background(), log))
				require.NoError(t, err)
				jobIdsByGangId = sch.jobIdsByGangId
				gangIdByJobId = sch.gangIdByJobId

				// Test resource accounting.
				for _, job := range result.PreemptedJobs {
					req := PodRequirementFromLegacySchedulerJob(job, tc.SchedulingConfig.Preemption.PriorityClasses)
					requests := schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests)
					quantityByPriorityAndResourceType := schedulerobjects.QuantityByPriorityAndResourceType{
						req.Priority: requests,
					}
					allocatedByQueueAndPriority[job.GetQueue()].Sub(quantityByPriorityAndResourceType)
				}
				for _, job := range result.ScheduledJobs {
					req := PodRequirementFromLegacySchedulerJob(job, tc.SchedulingConfig.Preemption.PriorityClasses)
					requests := schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests)
					quantityByPriorityAndResourceType := schedulerobjects.QuantityByPriorityAndResourceType{
						req.Priority: requests,
					}
					m := allocatedByQueueAndPriority[job.GetQueue()]
					if m == nil {
						m = make(schedulerobjects.QuantityByPriorityAndResourceType)
					}
					m.Add(quantityByPriorityAndResourceType)
					allocatedByQueueAndPriority[job.GetQueue()] = m
				}
				for queue, allocated := range allocatedByQueueAndPriority {
					// Filter out explicit zeros to enable comparing with expected allocation.
					allocatedByQueueAndPriority[queue] = armadamaps.Filter(
						allocated,
						func(_ int32, rl schedulerobjects.ResourceList) bool {
							return !rl.IsZero()
						},
					)
				}

				// Test that jobs are mapped to nodes correctly.
				for _, job := range result.PreemptedJobs {
					nodeId, ok := result.NodeIdByJobId[job.GetId()]
					assert.True(t, ok)
					assert.NotEmpty(t, nodeId)

					// Check that preempted jobs are preempted from the node they were previously scheduled onto.
					expectedNodeId := nodeIdByJobId[job.GetId()]
					assert.Equal(t, expectedNodeId, nodeId, "job %s preempted from unexpected node", job.GetId())
				}
				for _, job := range result.ScheduledJobs {
					nodeId, ok := result.NodeIdByJobId[job.GetId()]
					assert.True(t, ok)
					assert.NotEmpty(t, nodeId)

					// Check that scheduled jobs are consistently assigned to the same node.
					// (We don't allow moving jobs between nodes.)
					if expectedNodeId, ok := nodeIdByJobId[job.GetId()]; ok {
						assert.Equal(t, expectedNodeId, nodeId, "job %s scheduled onto unexpected node", job.GetId())
					} else {
						nodeIdByJobId[job.GetId()] = nodeId
					}
				}
				for jobId, nodeId := range result.NodeIdByJobId {
					if expectedNodeId, ok := nodeIdByJobId[jobId]; ok {
						assert.Equal(t, expectedNodeId, nodeId, "job %s preempted from/scheduled onto unexpected node", jobId)
					}
				}

				// Expected scheduled jobs.
				jobIdsByQueue := jobIdsByQueueFromJobs(result.ScheduledJobs)
				scheduledQueues := armadamaps.MapValues(round.ExpectedScheduledIndices, func(v []int) bool { return true })
				maps.Copy(scheduledQueues, armadamaps.MapValues(jobIdsByQueue, func(v []string) bool { return true }))
				for queue := range scheduledQueues {
					expected := round.ExpectedScheduledIndices[queue]
					jobIds := jobIdsByQueue[queue]
					actual := make([]int, 0)
					for _, jobId := range jobIds {
						actual = append(actual, indexByJobId[jobId])
					}
					slices.Sort(actual)
					slices.Sort(expected)
					assert.Equal(t, expected, actual, "scheduling from queue %s", queue)
				}

				// Expected preempted jobs.
				jobIdsByQueue = jobIdsByQueueFromJobs(result.PreemptedJobs)
				preemptedQueues := armadamaps.MapValues(round.ExpectedPreemptedIndices, func(v map[int][]int) bool { return true })
				maps.Copy(preemptedQueues, armadamaps.MapValues(jobIdsByQueue, func(v []string) bool { return true }))
				for queue := range preemptedQueues {
					expected := round.ExpectedPreemptedIndices[queue]
					jobIds := jobIdsByQueue[queue]
					actual := make(map[int][]int)
					for _, jobId := range jobIds {
						i := roundByJobId[jobId]
						j := indexByJobId[jobId]
						actual[i] = append(actual[i], j)
					}
					for _, s := range expected {
						slices.Sort(s)
					}
					for _, s := range actual {
						slices.Sort(s)
					}
					assert.Equal(t, expected, actual, "preempting from queue %s", queue)
				}

				// We expect there to be no oversubscribed nodes.
				prioritiesByName := configuration.PriorityByPriorityClassName(testfixtures.TestPriorityClasses)
				priorities := maps.Values(prioritiesByName)
				slices.Sort(priorities)
				it, err := nodedb.NewNodesIterator(nodeDb.Txn(false))
				require.NoError(t, err)
				for node := it.NextNode(); node != nil; node = it.NextNode() {
					for _, p := range priorities {
						for resourceType, q := range node.AllocatableByPriorityAndResource[p].Resources {
							assert.NotEqual(t, -1, q.Cmp(resource.Quantity{}), "resource %s oversubscribed by %s on node %s", resourceType, q.String(), node.Id)
						}
					}
				}
			}
		})
	}
}

func jobIdsByQueueFromJobs(jobs []interfaces.LegacySchedulerJob) map[string][]string {
	rv := make(map[string][]string)
	for _, job := range jobs {
		rv[job.GetQueue()] = append(rv[job.GetQueue()], job.GetId())
	}
	return rv
}

func BenchmarkPreemptingQueueScheduler(b *testing.B) {
	tests := map[string]struct {
		SchedulingConfig  configuration.SchedulingConfig
		Nodes             []*schedulerobjects.Node
		JobFunc           func(queue string, priorityClassName string, n int) []*jobdb.Job
		NumQueues         int
		NumJobsPerQueue   int
		MinimumJobSize    map[string]resource.Quantity
		MinPriorityFactor int
		MaxPriorityFactor int
	}{
		"1 node 1 queue 32 jobs": {
			SchedulingConfig: testfixtures.WithNodeOversubscriptionEvictionProbabilityConfig(
				0,
				testfixtures.WithNodeEvictionProbabilityConfig(
					0.1,
					testfixtures.TestSchedulingConfig(),
				),
			),
			Nodes:             testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			JobFunc:           testfixtures.NSmallCpuJob,
			NumQueues:         1,
			NumJobsPerQueue:   32,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 1,
		},
		"10 nodes 1 queue 320 jobs": {
			SchedulingConfig: testfixtures.WithNodeEvictionProbabilityConfig(
				0.1,
				testfixtures.TestSchedulingConfig(),
			),
			Nodes:             testfixtures.TestNCpuNode(10, testfixtures.TestPriorities),
			JobFunc:           testfixtures.NSmallCpuJob,
			NumQueues:         1,
			NumJobsPerQueue:   320,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 1,
		},
		"100 nodes 1 queue 3200 jobs": {
			SchedulingConfig: testfixtures.WithNodeEvictionProbabilityConfig(
				0.1,
				testfixtures.TestSchedulingConfig(),
			),
			Nodes:             testfixtures.TestNCpuNode(100, testfixtures.TestPriorities),
			JobFunc:           testfixtures.NSmallCpuJob,
			NumQueues:         1,
			NumJobsPerQueue:   3200,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 1,
		},
		"1000 nodes 1 queue 32000 jobs": {
			SchedulingConfig: testfixtures.WithNodeEvictionProbabilityConfig(
				0.1,
				testfixtures.TestSchedulingConfig(),
			),
			Nodes:             testfixtures.TestNCpuNode(1000, testfixtures.TestPriorities),
			JobFunc:           testfixtures.NSmallCpuJob,
			NumQueues:         1,
			NumJobsPerQueue:   32000,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 1,
		},
		"mixed": {
			SchedulingConfig: testfixtures.WithNodeEvictionProbabilityConfig(
				0.1,
				testfixtures.TestSchedulingConfig(),
			),
			Nodes:             testfixtures.TestNCpuNode(500, testfixtures.TestPriorities),
			JobFunc:           testfixtures.NSmallCpuJob,
			NumQueues:         100,
			NumJobsPerQueue:   256,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 10,
		},
	}
	for name, tc := range tests {
		b.Run(name, func(b *testing.B) {
			jobsByQueue := make(map[string][]*jobdb.Job)
			priorityFactorByQueue := make(map[string]float64)
			for i := 0; i < tc.NumQueues; i++ {
				queue := fmt.Sprintf("%d", i)
				jobsByQueue[queue] = tc.JobFunc(queue, testfixtures.PriorityClass0, tc.NumJobsPerQueue)
				priorityFactorByQueue[queue] = float64(rand.Intn(tc.MaxPriorityFactor-tc.MinPriorityFactor+1) + tc.MinPriorityFactor)
			}

			nodeDb, err := CreateNodeDb(tc.Nodes)
			require.NoError(b, err)
			repo := NewInMemoryJobRepository(testfixtures.TestPriorityClasses)
			usageByQueue := make(map[string]schedulerobjects.QuantityByPriorityAndResourceType)

			jobs := make([]interfaces.LegacySchedulerJob, 0)
			for _, queueJobs := range jobsByQueue {
				for _, job := range queueJobs {
					jobs = append(jobs, job)
				}
			}
			repo.EnqueueMany(jobs)

			sctx := schedulercontext.NewSchedulingContext(
				"executor",
				"pool",
				tc.SchedulingConfig.Preemption.PriorityClasses,
				tc.SchedulingConfig.Preemption.DefaultPriorityClass,
				tc.SchedulingConfig.ResourceScarcity,
				priorityFactorByQueue,
				nodeDb.TotalResources(),
				usageByQueue,
			)
			constraints := schedulerconstraints.SchedulingConstraintsFromSchedulingConfig(
				schedulerobjects.ResourceList{Resources: tc.MinimumJobSize},
				tc.SchedulingConfig,
			)
			sch := NewPreemptingQueueScheduler(
				sctx,
				constraints,
				tc.SchedulingConfig.Preemption.NodeEvictionProbability,
				tc.SchedulingConfig.Preemption.NodeOversubscriptionEvictionProbability,
				repo,
				nodeDb,
				nil,
				nil,
				nil,
			)
			sch.SkipUnsuccessfulSchedulingKeyCheck()
			result, err := sch.Schedule(
				ctxlogrus.ToContext(
					context.Background(),
					logrus.NewEntry(logrus.New()),
				),
			)
			require.NoError(b, err)
			require.Equal(b, 0, len(result.PreemptedJobs))

			// Create a new job repo without the scheduled jobs.
			scheduledJobsById := make(map[string]interfaces.LegacySchedulerJob)
			for _, job := range result.ScheduledJobs {
				scheduledJobsById[job.GetId()] = job
			}
			unscheduledJobs := make([]interfaces.LegacySchedulerJob, 0)
			for _, job := range jobs {
				if _, ok := scheduledJobsById[job.GetId()]; !ok {
					unscheduledJobs = append(unscheduledJobs, job)
				}
			}
			repo = NewInMemoryJobRepository(testfixtures.TestPriorityClasses)
			repo.EnqueueMany(unscheduledJobs)

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				sctx := schedulercontext.NewSchedulingContext(
					"executor",
					"pool",
					tc.SchedulingConfig.Preemption.PriorityClasses,
					tc.SchedulingConfig.Preemption.DefaultPriorityClass,
					tc.SchedulingConfig.ResourceScarcity,
					priorityFactorByQueue,
					nodeDb.TotalResources(),
					usageByQueue,
				)
				rescheduler := NewPreemptingQueueScheduler(
					sctx,
					constraints,
					tc.SchedulingConfig.Preemption.NodeEvictionProbability,
					tc.SchedulingConfig.Preemption.NodeOversubscriptionEvictionProbability,
					repo,
					nodeDb,
					nil,
					nil,
					nil,
				)
				result, err := rescheduler.Schedule(
					ctxlogrus.ToContext(
						context.Background(),
						logrus.NewEntry(logrus.New()),
					),
				)
				require.NoError(b, err)

				// We expect the system to be in steady-state, i.e., no preempted/scheduled jobs.
				require.Equal(b, 0, len(result.PreemptedJobs))
				require.Equal(b, 0, len(result.ScheduledJobs))
			}
		})
	}
}
