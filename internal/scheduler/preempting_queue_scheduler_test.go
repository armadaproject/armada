package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/fairness"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestEvictOversubscribed(t *testing.T) {
	jobs := append(
		testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 20),
		testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 20)...,
	)
	node := testfixtures.Test32CpuNode(testfixtures.TestPriorities)
	nodeDb, err := NewNodeDb()
	require.NoError(t, err)
	txn := nodeDb.Txn(true)
	err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, jobs, node)
	require.NoError(t, err)
	entry, err := nodeDb.GetNode(node.Id)
	require.NoError(t, err)

	jobRepo := NewInMemoryJobRepository(testfixtures.TestPriorityClasses)
	for _, job := range jobs {
		jobRepo.Enqueue(job)
	}
	evictor := NewOversubscribedEvictor(
		jobRepo,
		testfixtures.TestPriorityClasses,
		testfixtures.TestDefaultPriorityClass,
		1,
		nil,
	)
	it := NewInMemoryNodeIterator([]*nodedb.Node{entry})
	result, err := evictor.Evict(context.Background(), it)
	require.NoError(t, err)

	prioritiesByName := configuration.PriorityByPriorityClassName(testfixtures.TestPriorityClasses)
	priorities := maps.Values(prioritiesByName)
	slices.Sort(priorities)
	for nodeId, node := range result.AffectedNodesById {
		for _, p := range priorities {
			for resourceType, q := range node.AllocatableByPriority[p].Resources {
				assert.NotEqual(t, -1, q.Cmp(resource.Quantity{}), "resource %s oversubscribed by %s on node %s", resourceType, q.String(), nodeId)
			}
		}
	}
}

type InMemoryNodeIterator struct {
	i     int
	nodes []*nodedb.Node
}

func NewInMemoryNodeIterator(nodes []*nodedb.Node) *InMemoryNodeIterator {
	return &InMemoryNodeIterator{
		nodes: slices.Clone(nodes),
	}
}

func (it *InMemoryNodeIterator) NextNode() *nodedb.Node {
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
		// Indices of nodes that should be cordoned before scheduling.
		NodeIndicesToCordon []int
	}
	tests := map[string]struct {
		SchedulingConfig configuration.SchedulingConfig
		// Nodes to be considered by the scheduler.
		Nodes []*schedulerobjects.Node
		// Each item corresponds to a call to Reschedule().
		Rounds []SchedulingRound
		// Map from queue to the priority factor associated with that queue.
		PriorityFactorByQueue map[string]float64
		// Initial resource usage for all queues. This value is used across all rounds,
		// i.e., we don't update it based on preempted/scheduled jobs.
		InitialAllocationByQueueAndPriorityClass map[string]schedulerobjects.QuantityByTAndResourceType[string]
		// Total resources across all clusters.
		// If empty, it is computed as the total resources across the provided nodes.
		TotalResources schedulerobjects.ResourceList
		// Minimum job size.
		MinimumJobSize map[string]resource.Quantity
	}{
		"balancing three queues": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 32),
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
						"C": testfixtures.N1Cpu4GiJobs("C", testfixtures.PriorityClass0, 10),
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
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 1),
						"C": testfixtures.N1Cpu4GiJobs("C", testfixtures.PriorityClass0, 1),
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
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 32),
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
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 1),
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
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 32),
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
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 1),
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
			Nodes:            testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 32),
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
			Nodes:            testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
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
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2, 33),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass3, 1),
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
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2, 1),
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
			Nodes:            testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					// These should all be scheduled onto the second node with no preemptions necessary.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass1, 1),
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
			Nodes:            testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass1, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					// This job should preempt the priority-0 jobs.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass2, 1),
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
			Nodes:            testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass1, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					// These should all be scheduled onto the second node with no preemptions necessary.
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N32Cpu256GiJobs("B", testfixtures.PriorityClass0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 0),
					},
				},
				{
					// These should all be scheduled onto the second node with no preemptions necessary.
					JobsByQueue: map[string][]*jobdb.Job{
						"C": testfixtures.N32Cpu256GiJobs("C", testfixtures.PriorityClass2, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"C": testfixtures.IntRange(0, 0),
					},
				},
				{
					// These should all be scheduled onto the second node with no preemptions necessary.
					JobsByQueue: map[string][]*jobdb.Job{
						"D": testfixtures.N32Cpu256GiJobs("D", testfixtures.PriorityClass3, 1),
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
			Nodes:            testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					// Fill half of node 1 and half of node 2.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 16),
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 16),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 15),
						"B": testfixtures.IntRange(0, 15),
					},
				},
				{
					// Schedule a gang filling the remaining space on both nodes.
					JobsByQueue: map[string][]*jobdb.Job{
						"C": testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("C", testfixtures.PriorityClass0, 32)),
					},
					ExpectedScheduledIndices: map[string][]int{
						"C": testfixtures.IntRange(0, 31),
					},
				},
				{
					// Schedule jobs that requires preempting one job in the gang,
					// and assert that all jobs in the gang are preempted.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 17),
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
			Nodes:            testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					// Schedule a gang across two nodes.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.WithGangAnnotationsJobs(testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 2)),
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
			Nodes: testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					// Schedule a gang filling all of node 1 and part of node 2.
					// Make the jobs of node 1 priority 1,
					// to avoid them being urgency-preempted in the next round.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.WithGangAnnotationsJobs(
							append(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 32), testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)...),
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
						"B": testfixtures.N32Cpu256GiJobs("B", testfixtures.PriorityClass1, 1),
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
			Nodes: testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					// Schedule a gang spanning nodes 1 and 2.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 33)),
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
							append(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 31), testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)...),
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
						"B": testfixtures.N32Cpu256GiJobs("B", testfixtures.PriorityClass1, 1),
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
		// "rescheduled jobs don't count towards maxJobsToSchedule": {
		// 	SchedulingConfig: testfixtures.WithMaxJobsToScheduleConfig(5, testfixtures.TestSchedulingConfig()),
		// 	Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
		// 	Rounds: []SchedulingRound{
		// 		{
		// 			JobsByQueue: map[string][]*jobdb.Job{
		// 				"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 10),
		// 			},
		// 			ExpectedScheduledIndices: map[string][]int{
		// 				"A": testfixtures.IntRange(0, 4),
		// 			},
		// 		},
		// 		{
		// 			JobsByQueue: map[string][]*jobdb.Job{
		// 				"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 10),
		// 			},
		// 			ExpectedScheduledIndices: map[string][]int{
		// 				"A": testfixtures.IntRange(0, 4),
		// 			},
		// 		},
		// 	},
		// 	PriorityFactorByQueue: map[string]float64{
		// 		"A": 1,
		// 	},
		// },
		"rescheduled jobs don't count towards maxQueueLookback": {
			SchedulingConfig: testfixtures.WithMaxLookbackPerQueueConfig(5, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 2),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 1),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 10),
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
		"rescheduled jobs don't count towards MaximumClusterFractionToSchedule": {
			SchedulingConfig: testfixtures.WithRoundLimitsConfig(
				map[string]float64{
					"cpu": 5.0 / 32.0,
				},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 5),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 5),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"priority class preemption two classes": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass1, 1),
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
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N32Cpu256GiJobs("B", testfixtures.PriorityClass1, 1),
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
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": append(testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1), testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass1, 1)...),
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
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": append(append(
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 10),
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 10)...),
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2, 10)...,
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 29),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass3, 24),
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
		"MaximumResourceFractionPerQueue": {
			SchedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[string]map[string]float64{
					testfixtures.PriorityClass0: {"cpu": 1.0 / 32.0},
					testfixtures.PriorityClass1: {"cpu": 2.0 / 32.0},
					testfixtures.PriorityClass2: {"cpu": 3.0 / 32.0},
					testfixtures.PriorityClass3: {"cpu": 4.0 / 32.0},
				},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": armadaslices.Concatenate(
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 32),
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2, 32),
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass3, 32),
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": {0, 32, 33, 64, 65, 66, 96, 97, 98, 99},
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"MaximumResourceFractionPerQueue multiple rounds": {
			SchedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[string]map[string]float64{
					testfixtures.PriorityClass0: {"cpu": 1.0 / 32.0},
					testfixtures.PriorityClass1: {"cpu": 2.0 / 32.0},
					testfixtures.PriorityClass2: {"cpu": 3.0 / 32.0},
					testfixtures.PriorityClass3: {"cpu": 4.0 / 32.0},
				},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": armadaslices.Concatenate(
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": armadaslices.Concatenate(
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 32),
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 1),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": armadaslices.Concatenate(
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2, 32),
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 32),
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 2),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": armadaslices.Concatenate(
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass3, 32),
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2, 32),
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 32),
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 3),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": armadaslices.Concatenate(
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass3, 32),
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2, 32),
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 32),
							testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
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
				map[string]float64{"cpu": 15.0 / 64.0},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 15),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 64),
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 15),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 64),
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 7),
						"B": testfixtures.IntRange(0, 7),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 64),
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 64),
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
		"Queued jobs are not preempted cross queue": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass1, 32),
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
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass1, 31),
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
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass3, 32),
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
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 16),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 15),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 16),
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass1, 32),
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
		"Oversubscribed eviction does not evict non-preemptible": {
			SchedulingConfig: testfixtures.WithNodeEvictionProbabilityConfig(
				0.0,
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": armadaslices.Concatenate(
							testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass2, 1),
							testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass2NonPreemptible, 3),
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 3),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": armadaslices.Concatenate(
							testfixtures.N16Cpu128GiJobs("B", testfixtures.PriorityClass3, 1),
							testfixtures.N16Cpu128GiJobs("B", testfixtures.PriorityClass2NonPreemptible, 1),
						),
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
				{}, // Empty round to make sure nothing changes.
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"Cordoning prevents scheduling new jobs but not re-scheduling running jobs": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass1, 1),
					},
					NodeIndicesToCordon: []int{0},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass1, 1),
					},
				},
				{}, // Empty round to make sure nothing changes.
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"ProtectedFractionOfFairShare": {
			SchedulingConfig: testfixtures.WithProtectedFractionOfFairShareConfig(
				1.0,
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 9),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass3, 22),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 21),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"C": testfixtures.N1Cpu4GiJobs("C", testfixtures.PriorityClass0, 1),
					},
				},
				{}, // Empty round to make sure nothing changes.
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
				"C": 1,
			},
		},
		"ProtectedFractionOfFairShare at limit": {
			SchedulingConfig: testfixtures.WithProtectedFractionOfFairShareConfig(
				0.5,
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 8),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 7),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass3, 24),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 23),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"C": testfixtures.N1Cpu4GiJobs("C", testfixtures.PriorityClass0, 1),
					},
				},
				{}, // Empty round to make sure nothing changes.
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 0.5,
				"B": 1,
				"C": 1,
			},
		},
		"ProtectedFractionOfFairShare above limit": {
			SchedulingConfig: testfixtures.WithProtectedFractionOfFairShareConfig(
				0.5,
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 9),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 8),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass3, 23),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 22),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"C": testfixtures.N1Cpu4GiJobs("C", testfixtures.PriorityClass0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"C": testfixtures.IntRange(0, 0),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(8, 8),
						},
					},
				},
				{}, // Empty round to make sure nothing changes.
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
				"C": 1,
			},
		},
		"DominantResourceFairness": {
			SchedulingConfig: testfixtures.WithDominantResourceFairnessConfig(
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu16GiJobs("A", testfixtures.PriorityClass0, 32),
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 9),
						"B": testfixtures.IntRange(0, 21),
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
			// balancing three queues
			// gang_preemption_with_partial_gang
			fmt.Println("name", name)
			if name != "gang preemption with partial gang" {
				return
			}

			nodeDb, err := NewNodeDb()
			require.NoError(t, err)
			txn := nodeDb.Txn(true)
			for _, node := range tc.Nodes {
				err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node)
				require.NoError(t, err)
			}
			txn.Commit()

			// Repo. for storing jobs to be queued.
			// The Redis job repo. doesn't order by pc, so we disable pc ordering here too.
			repo := NewInMemoryJobRepository(testfixtures.TestPriorityClasses)
			repo.sortByPriorityClass = false

			// Accounting across scheduling rounds.
			roundByJobId := make(map[string]int)
			indexByJobId := make(map[string]int)
			allocatedByQueueAndPriorityClass := armadamaps.DeepCopy(tc.InitialAllocationByQueueAndPriorityClass)
			nodeIdByJobId := make(map[string]string)
			var jobIdsByGangId map[string]map[string]bool
			var gangIdByJobId map[string]string

			// Scheduling rate-limiters persist between rounds.
			// We control the rate at which time passes between scheduling rounds.
			schedulingInterval := time.Second
			limiter := rate.NewLimiter(
				rate.Limit(tc.SchedulingConfig.MaximumSchedulingRate),
				tc.SchedulingConfig.MaximumSchedulingBurst,
			)
			limiterByQueue := make(map[string]*rate.Limiter)
			for queue := range tc.PriorityFactorByQueue {
				limiterByQueue[queue] = rate.NewLimiter(
					rate.Limit(tc.SchedulingConfig.MaximumPerQueueSchedulingRate),
					tc.SchedulingConfig.MaximumPerQueueSchedulingBurst,
				)
			}

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
							nodeId := nodeIdByJobId[job.GetId()]
							node, err := nodeDb.GetNode(nodeId)
							require.NoError(t, err)
							node, err = nodedb.UnbindJobFromNode(tc.SchedulingConfig.Preemption.PriorityClasses, job, node)
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

				// Cordon nodes.
				for _, j := range round.NodeIndicesToCordon {
					node, err := nodeDb.GetNode(tc.Nodes[j].Id)
					require.NoError(t, err)
					node = node.UnsafeCopy()
					node.Taints = append(slices.Clone(node.Taints), nodedb.UnschedulableTaint())
					err = nodeDb.Upsert(node)
					require.NoError(t, err)
				}

				// If not provided, set total resources equal to the aggregate over tc.Nodes.
				if tc.TotalResources.Resources == nil {
					tc.TotalResources = nodeDb.TotalResources()
				}

				fairnessCostProvider, err := fairness.NewDominantResourceFairness(
					nodeDb.TotalResources(),
					tc.SchedulingConfig.DominantResourceFairnessResourcesToConsider,
				)
				require.NoError(t, err)
				sctx := schedulercontext.NewSchedulingContext(
					"executor",
					"pool",
					tc.SchedulingConfig.Preemption.PriorityClasses,
					tc.SchedulingConfig.Preemption.DefaultPriorityClass,
					fairnessCostProvider,
					limiter,
					tc.TotalResources,
				)
				sctx.Started = time.Time{}.Add(time.Duration(i) * schedulingInterval)
				for queue, priorityFactor := range tc.PriorityFactorByQueue {
					weight := 1 / priorityFactor
					err := sctx.AddQueueSchedulingContext(
						queue,
						weight,
						allocatedByQueueAndPriorityClass[queue],
						limiterByQueue[queue],
					)
					require.NoError(t, err)
				}
				constraints := schedulerconstraints.SchedulingConstraintsFromSchedulingConfig(
					"pool",
					tc.TotalResources,
					schedulerobjects.ResourceList{Resources: tc.MinimumJobSize},
					tc.SchedulingConfig,
				)
				sch := NewPreemptingQueueScheduler(
					sctx,
					constraints,
					tc.SchedulingConfig.Preemption.NodeEvictionProbability,
					tc.SchedulingConfig.Preemption.NodeOversubscriptionEvictionProbability,
					tc.SchedulingConfig.Preemption.ProtectedFractionOfFairShare,
					repo,
					nodeDb,
					nodeIdByJobId,
					jobIdsByGangId,
					gangIdByJobId,
				)
				sch.EnableAssertions()
				if tc.SchedulingConfig.EnableNewPreemptionStrategy {
					sch.EnableNewPreemptionStrategy()
				}
				result, err := sch.Schedule(ctxlogrus.ToContext(context.Background(), log))
				require.NoError(t, err)
				jobIdsByGangId = sch.jobIdsByGangId
				gangIdByJobId = sch.gangIdByJobId

				// Test resource accounting.
				for _, job := range result.PreemptedJobs {
					m := allocatedByQueueAndPriorityClass[job.GetQueue()]
					if m == nil {
						m = make(schedulerobjects.QuantityByTAndResourceType[string])
						allocatedByQueueAndPriorityClass[job.GetQueue()] = m
					}
					m.SubV1ResourceList(
						job.GetPriorityClassName(),
						job.GetResourceRequirements().Requests,
					)
				}
				for _, job := range result.ScheduledJobs {
					m := allocatedByQueueAndPriorityClass[job.GetQueue()]
					if m == nil {
						m = make(schedulerobjects.QuantityByTAndResourceType[string])
						allocatedByQueueAndPriorityClass[job.GetQueue()] = m
					}
					m.AddV1ResourceList(
						job.GetPriorityClassName(),
						job.GetResourceRequirements().Requests,
					)
				}
				for queue, qctx := range sctx.QueueSchedulingContexts {
					assert.True(t, qctx.AllocatedByPriorityClass.Equal(allocatedByQueueAndPriorityClass[queue]))
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
						for resourceType, q := range node.AllocatableByPriority[p].Resources {
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
		"1 node 1 queue 320 jobs": {
			SchedulingConfig:  testfixtures.TestSchedulingConfig(),
			Nodes:             testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			JobFunc:           testfixtures.N1Cpu4GiJobs,
			NumQueues:         1,
			NumJobsPerQueue:   320,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 1,
		},
		"1 node 10 queues 320 jobs": {
			SchedulingConfig:  testfixtures.TestSchedulingConfig(),
			Nodes:             testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			JobFunc:           testfixtures.N1Cpu4GiJobs,
			NumQueues:         10,
			NumJobsPerQueue:   320,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 1,
		},
		"10 nodes 1 queue 3200 jobs": {
			SchedulingConfig:  testfixtures.TestSchedulingConfig(),
			Nodes:             testfixtures.N32CpuNodes(10, testfixtures.TestPriorities),
			JobFunc:           testfixtures.N1Cpu4GiJobs,
			NumQueues:         1,
			NumJobsPerQueue:   3200,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 1,
		},
		"10 nodes 10 queues 3200 jobs": {
			SchedulingConfig:  testfixtures.TestSchedulingConfig(),
			Nodes:             testfixtures.N32CpuNodes(10, testfixtures.TestPriorities),
			JobFunc:           testfixtures.N1Cpu4GiJobs,
			NumQueues:         10,
			NumJobsPerQueue:   3200,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 1,
		},
		"100 nodes 1 queue 32000 jobs": {
			SchedulingConfig:  testfixtures.TestSchedulingConfig(),
			Nodes:             testfixtures.N32CpuNodes(100, testfixtures.TestPriorities),
			JobFunc:           testfixtures.N1Cpu4GiJobs,
			NumQueues:         1,
			NumJobsPerQueue:   32000,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 1,
		},
		"100 nodes 10 queues 32000 jobs": {
			SchedulingConfig:  testfixtures.TestSchedulingConfig(),
			Nodes:             testfixtures.N32CpuNodes(100, testfixtures.TestPriorities),
			JobFunc:           testfixtures.N1Cpu4GiJobs,
			NumQueues:         10,
			NumJobsPerQueue:   32000,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 1,
		},
		"1000 nodes 1 queue 320000 jobs": {
			SchedulingConfig:  testfixtures.TestSchedulingConfig(),
			Nodes:             testfixtures.N32CpuNodes(1000, testfixtures.TestPriorities),
			JobFunc:           testfixtures.N1Cpu4GiJobs,
			NumQueues:         1,
			NumJobsPerQueue:   320000,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 1,
		},
		"1000 nodes 10 queues 320000 jobs": {
			SchedulingConfig:  testfixtures.TestSchedulingConfig(),
			Nodes:             testfixtures.N32CpuNodes(1000, testfixtures.TestPriorities),
			JobFunc:           testfixtures.N1Cpu4GiJobs,
			NumQueues:         1,
			NumJobsPerQueue:   32000,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 1,
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

			nodeDb, err := NewNodeDb()
			require.NoError(b, err)
			txn := nodeDb.Txn(true)
			for _, node := range tc.Nodes {
				err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node)
				require.NoError(b, err)
			}
			txn.Commit()

			jobRepo := NewInMemoryJobRepository(testfixtures.TestPriorityClasses)

			jobs := make([]interfaces.LegacySchedulerJob, 0)
			for _, queueJobs := range jobsByQueue {
				for _, job := range queueJobs {
					jobs = append(jobs, job)
				}
			}
			jobRepo.EnqueueMany(jobs)

			fairnessCostProvider, err := fairness.NewDominantResourceFairness(
				nodeDb.TotalResources(),
				tc.SchedulingConfig.DominantResourceFairnessResourcesToConsider,
			)
			require.NoError(b, err)
			sctx := schedulercontext.NewSchedulingContext(
				"executor",
				"pool",
				tc.SchedulingConfig.Preemption.PriorityClasses,
				tc.SchedulingConfig.Preemption.DefaultPriorityClass,
				fairnessCostProvider,
				nil,
				nodeDb.TotalResources(),
			)
			for queue, priorityFactor := range priorityFactorByQueue {
				weight := 1 / priorityFactor
				err := sctx.AddQueueSchedulingContext(queue, weight, make(schedulerobjects.QuantityByTAndResourceType[string]), nil)
				require.NoError(b, err)
			}
			constraints := schedulerconstraints.SchedulingConstraintsFromSchedulingConfig(
				"pool",
				nodeDb.TotalResources(),
				schedulerobjects.ResourceList{Resources: tc.MinimumJobSize},
				tc.SchedulingConfig,
			)
			sch := NewPreemptingQueueScheduler(
				sctx,
				constraints,
				tc.SchedulingConfig.Preemption.NodeEvictionProbability,
				tc.SchedulingConfig.Preemption.NodeOversubscriptionEvictionProbability,
				tc.SchedulingConfig.Preemption.ProtectedFractionOfFairShare,
				jobRepo,
				nodeDb,
				nil,
				nil,
				nil,
			)
			result, err := sch.Schedule(context.Background())
			require.NoError(b, err)
			require.Equal(b, 0, len(result.PreemptedJobs))

			scheduledJobs := make(map[string]bool)
			for _, job := range result.ScheduledJobs {
				scheduledJobs[job.GetId()] = true
			}
			for queue, jobs := range jobRepo.jobsByQueue {
				jobRepo.jobsByQueue[queue] = armadaslices.Filter(jobs, func(job interfaces.LegacySchedulerJob) bool { return scheduledJobs[job.GetId()] })
			}

			jobsByNodeId := make(map[string][]*jobdb.Job)
			for _, job := range ScheduledJobsFromSchedulerResult[*jobdb.Job](result) {
				nodeId := result.NodeIdByJobId[job.GetId()]
				jobsByNodeId[nodeId] = append(jobsByNodeId[nodeId], job)
			}
			nodeDb, err = NewNodeDb()
			require.NoError(b, err)
			txn = nodeDb.Txn(true)
			for _, node := range tc.Nodes {
				err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, jobsByNodeId[node.Id], node)
				require.NoError(b, err)
			}
			txn.Commit()

			allocatedByQueueAndPriorityClass := sctx.AllocatedByQueueAndPriority()

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				sctx := schedulercontext.NewSchedulingContext(
					"executor",
					"pool",
					tc.SchedulingConfig.Preemption.PriorityClasses,
					tc.SchedulingConfig.Preemption.DefaultPriorityClass,
					fairnessCostProvider,
					nil,
					nodeDb.TotalResources(),
				)
				for queue, priorityFactor := range priorityFactorByQueue {
					weight := 1 / priorityFactor
					err := sctx.AddQueueSchedulingContext(queue, weight, allocatedByQueueAndPriorityClass[queue], nil)
					require.NoError(b, err)
				}
				sch := NewPreemptingQueueScheduler(
					sctx,
					constraints,
					tc.SchedulingConfig.Preemption.NodeEvictionProbability,
					tc.SchedulingConfig.Preemption.NodeOversubscriptionEvictionProbability,
					tc.SchedulingConfig.Preemption.ProtectedFractionOfFairShare,
					jobRepo,
					nodeDb,
					nil,
					nil,
					nil,
				)
				result, err := sch.Schedule(context.Background())
				require.NoError(b, err)

				// We expect the system to be in steady-state, i.e., no preempted/scheduled jobs.
				require.Equal(b, 0, len(result.PreemptedJobs))
				require.Equal(b, 0, len(result.ScheduledJobs))
			}
		})
	}
}
