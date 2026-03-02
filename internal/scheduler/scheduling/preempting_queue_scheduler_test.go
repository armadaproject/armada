package scheduling

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/logging"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	schedulingcontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

type testQueueContextChecker struct {
	jobIds map[string]bool
}

func (t testQueueContextChecker) QueueContextExists(job *jobdb.Job) bool {
	return t.jobIds[job.Id()]
}

func TestEvictOversubscribed(t *testing.T) {
	config := testfixtures.TestSchedulingConfig()

	priorities := types.AllowedPriorities(config.PriorityClasses)

	jobs := append(
		testfixtures.N1Cpu4GiJobs("A", config.DefaultPriorityClassName, 20),
		testfixtures.N1Cpu4GiJobs("A", config.DefaultPriorityClassName, 20)...,
	)

	stringInterner := stringinterner.New(1024)

	nodeDb, err := NewNodeDb(config, stringInterner)
	require.NoError(t, err)
	nodeDbTxn := nodeDb.Txn(true)
	err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(
		nodeDbTxn,
		jobs,
		testfixtures.Test32CpuNode(priorities),
	)
	require.NoError(t, err)

	jobDb := jobdb.NewJobDb(config.PriorityClasses, config.DefaultPriorityClassName, stringInterner, testfixtures.TestResourceListFactory)
	jobDbTxn := jobDb.WriteTxn()
	err = jobDbTxn.Upsert(jobs)
	require.NoError(t, err)

	evictor := NewOversubscribedEvictor(
		testQueueContextChecker{},
		jobDbTxn,
		nodeDb)
	result, err := evictor.Evict(armadacontext.Background(), nodeDbTxn)
	require.NoError(t, err)

	for nodeId, node := range result.AffectedNodesById {
		for _, p := range priorities {
			for _, r := range node.AllocatableByPriority[p].GetAll() {
				assert.False(t, r.IsNegative(), "resource oversubscribed by %s on node %s", r.String(), nodeId)
			}
		}
	}
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
		// If this round should run with the optimiser enabled
		OptimiserEnabled bool
	}
	tests := map[string]struct {
		SchedulingConfig configuration.SchedulingConfig
		// Nodes to be considered by the scheduler.
		Nodes []*internaltypes.Node
		// Each item corresponds to a call to Reschedule().
		Rounds []SchedulingRound
		// Map from queue to the priority factor associated with that queue.
		PriorityFactorByQueue map[string]float64
		// Map of nodeId to jobs running on those nodes
		InitialRunningJobs map[int][]*jobdb.Job
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
		"optimiser": {
			SchedulingConfig: testfixtures.WithProtectedFractionOfFairShareConfig(1,
				testfixtures.WithOptimiserConfig(testfixtures.TestPool, createOptimiserConfig(10, map[string]float64{}, nil),
					testfixtures.TestSchedulingConfig())),
			Nodes: armadaslices.Concatenate(
				testfixtures.NTainted32CpuNodes(1, testfixtures.TestPriorities), // Here to ensure B isn't above fairshare
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N32Cpu256GiJobs("B", testfixtures.PriorityClass2, 1),
					},
					ExpectedScheduledIndices: map[string][]int{},
					ExpectedPreemptedIndices: map[string]map[int][]int{},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N32Cpu256GiJobs("B", testfixtures.PriorityClass2, 1),
					},
					OptimiserEnabled: true,
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
		"don't prempt jobs where we don't know the queue": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 23),
					},
				},
			},
			InitialRunningJobs: map[int][]*jobdb.Job{
				0: testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass1, 8),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"avoid preemption when not improving fairness": {
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
						"B": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("B", testfixtures.PriorityClass0, 1),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"avoid preemption when not improving fairness reverse queue naming": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
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
						"A": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"preemption when improving fairness": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 63),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("B", testfixtures.PriorityClass0, 1),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(32, 63),
						},
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 0),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
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
		"urgency-based preemption - gangs": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32)),
						"B": testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass1, 32)),
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
		"avoid urgency-based preemption when possible": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					// These should all be scheduled onto the second node with no preemptions necessary.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass1, 1),
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
						"A": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass1, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					// This job should preempt the priority-0 jobs.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass2, 1),
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
		"avoid urgency-based preemption when possible cross-queue": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass1, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					// These should all be scheduled onto the second node with no preemptions necessary.
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("B", testfixtures.PriorityClass0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 0),
					},
				},
				{
					// These should all be scheduled onto the second node with no preemptions necessary.
					JobsByQueue: map[string][]*jobdb.Job{
						"C": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("C", testfixtures.PriorityClass2, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"C": testfixtures.IntRange(0, 0),
					},
				},
				{
					// These should all be scheduled onto the second node with no preemptions necessary.
					JobsByQueue: map[string][]*jobdb.Job{
						"D": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("D", testfixtures.PriorityClass3, 1),
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
		"gang scheduled - away": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.NTainted32CpuNodes(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.WithGangAnnotationsJobs(testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass4PreemptibleAway, 2)),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 1),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"gang scheduled - mixed home and away": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: armadaslices.Concatenate(
				testfixtures.NTainted32CpuNodes(1, testfixtures.TestPriorities), // Tainted so one job will schedule away
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.WithGangAnnotationsJobs(testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass4PreemptibleAway, 2)),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 1),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"gang scheduled mixed home and away - preempted in same round": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: armadaslices.Concatenate(
				testfixtures.NTainted32CpuNodes(1, testfixtures.TestPriorities), // Tainted so one job will schedule away
				testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			),
			Rounds: []SchedulingRound{
				{
					// Schedule queue B first, so it uses some share and schedule second in the second round
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N32Cpu256GiJobs("B", testfixtures.PriorityClass6Preemptible, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 0),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.WithGangAnnotationsJobs(testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass4PreemptibleAway, 2)),
						// Schedule a job for queue B that can run as "home" on the tainted nodes, so that it preempts the away schedule gang job
						"B": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("B", testfixtures.PriorityClass6Preemptible, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 0),
					},
				},
			},

			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
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

		"gang preemption avoid cascading preemption": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
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
						"B": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("B", testfixtures.PriorityClass1, 1),
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
		"rescheduled jobs don't count towards global scheduling rate limit": {
			SchedulingConfig: testfixtures.WithGlobalSchedulingRateLimiterConfig(2, 5, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 4),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 1),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"MaximumSchedulingRate": {
			SchedulingConfig: testfixtures.WithGlobalSchedulingRateLimiterConfig(2, 4, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 6)),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 3),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 10),
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
						"A": testfixtures.IntRange(0, 1),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{"A": 1},
		},
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
						"A": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass1, 1),
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
						"A": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 0),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N32Cpu256GiJobsWithLargeJobToleration("B", testfixtures.PriorityClass1, 1),
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
						"A": append(testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1), testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass1, 1)...),
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
		// - Define a 32 core node
		// - Completely fill it with 32 one-core jobs, half at a priority of zero and half at a priority of 1
		// - Schedule 17 more jobs on it at a priority of 2
		// - 17 jobs should be preempted to make space;  all the priority zero jobs and a single priority 1 job
		"priority class preemption through multiple levels": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 16),
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass1, 16),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 15),
						"B": testfixtures.IntRange(0, 15),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"C": testfixtures.N1Cpu4GiJobs("C", testfixtures.PriorityClass2, 17),
					},
					ExpectedScheduledIndices: map[string][]int{
						"C": testfixtures.IntRange(0, 16),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(0, 15),
						},
						"B": {
							0: testfixtures.IntRange(15, 15),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1, "B": 1, "C": 1,
			},
		},
		// This test shows how urgency preemption is broken if you have non-preepmtible priority classes.  What we do here is:
		// - Define a 32 core node
		// - Completely fill it with 32 one-core jobs at priority 2 with a priority class that can't be preempted.
		// - Schedule 16 more jobs on it at a priority of 3
		// - What should happen is that none of the new jobs are scheduled; the cluster is full of non-preemptible jobs.
		// - What actually happens is that *all* of the priority 3 jobs get scheduled but no jobs are preempted
		//   time="2024-09-27T09:09:05+01:00" level=info msg="Unbinding 0 preempted and 0 evicted jobs" round=1 stage=scheduling-algo
		//   time="2024-09-27T09:09:05+01:00" level=info msg="Finished unbinding preempted and evicted jobs" round=1 stage=scheduling-algo
		//   time="2024-09-27T09:09:05+01:00" level=info msg="Scheduling new jobs; affected queues [B]; resources map[B:{cpu: 16, memory: 64Gi}]; jobs per queue map[B:16]" round=1
		//"broken priority class preemption with non-preemptible priority classes": {
		//	SchedulingConfig: testfixtures.TestSchedulingConfig(),
		//	Nodes:            testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
		//	Rounds: []SchedulingRound{
		//		{
		//			JobsByQueue: map[string][]*jobdb.Job{
		//				"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2NonPreemptible, 32),
		//			},
		//			ExpectedScheduledIndices: map[string][]int{
		//				"A": testfixtures.IntRange(0, 31),
		//			},
		//		},
		//		{
		//			JobsByQueue: map[string][]*jobdb.Job{
		//				"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass3, 16),
		//			},
		//		},
		//	},
		//	PriorityFactorByQueue: map[string]float64{
		//		"A": 1, "B": 1, "C": 1,
		//	},
		//},
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
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
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
		"ProtectedFractionOfFairShare reshared": {
			SchedulingConfig: testfixtures.WithProtectedFractionOfFairShareConfig(
				1.0,
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2NonPreemptible, 16), // not preemptible
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 11),
						"C": testfixtures.N1Cpu4GiJobs("C", testfixtures.PriorityClass0, 3),
						"D": testfixtures.N1Cpu4GiJobs("D", testfixtures.PriorityClass0, 2),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 15),
						"B": testfixtures.IntRange(0, 10),
						"C": testfixtures.IntRange(0, 2),
						"D": testfixtures.IntRange(0, 1),
					},
				},
				{
					// D submits one more job. No preemption occurs because B is below adjusted fair share
					JobsByQueue: map[string][]*jobdb.Job{
						"D": testfixtures.N1Cpu4GiJobs("D", testfixtures.PriorityClass0, 1),
					},
				},
				{}, // Empty round to make sure nothing changes.
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
				"C": 1,
				"D": 1,
			},
		},
		"ProtectedFractionOfFairShare non equal weights": {
			SchedulingConfig: testfixtures.WithProtectedFractionOfFairShareConfig(
				1.0,
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2NonPreemptible, 24),
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 8),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 23),
						"B": testfixtures.IntRange(0, 7),
					},
				},
				{
					// D submits one more job. No preemption occurs because B is below adjusted fair share
					JobsByQueue: map[string][]*jobdb.Job{
						"C": testfixtures.N1Cpu4GiJobs("C", testfixtures.PriorityClass0, 1),
					},
				},
				{}, // Empty round to make sure nothing changes.
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 2,
				"C": 1,
			},
		},
		"DominantResourceFairness": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
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
		"nodeAffinity node notIn": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: armadaslices.Concatenate(
				testfixtures.TestNodeFactory.AddLabels(
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					map[string]string{"key": "val1"},
				),
				testfixtures.TestNodeFactory.AddLabels(
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					map[string]string{"key": "val2"},
				),
				testfixtures.TestNodeFactory.AddLabels(
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					map[string]string{"key": "val3"},
				),
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": armadaslices.Concatenate(
							testfixtures.WithNodeAffinityJobs(
								[]v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "key",
												Operator: v1.NodeSelectorOpNotIn,
												Values:   []string{"val1", "val2"},
											},
										},
									},
								},
								testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 3),
							),
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": {0, 1},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{"A": 1},
		},
		"SchedulingKey incorrect re-use": {
			// Two nodes. Fill both up each with half A and half B.
			// Schedule job from queue C.
			// This does not prevent re-scheduling.
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
						"B": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"C": testfixtures.N1Cpu4GiJobs("C", testfixtures.PriorityClass0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"C": testfixtures.IntRange(0, 20), // 21 jobs
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(22, 31), // 10 jobs
						},
						"B": {
							0: testfixtures.IntRange(21, 31), // 11 jobs
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{"A": 1, "B": 1, "C": 1},
		},
		"SchedulingKey incorrect re-use with cordoning": {
			// Fill a node with jobs from queue A.
			// Cordon the node.
			// Try to schedule a job from queue B. This fails as the node is cordoned.
			// This should not prevent re-scheduling jobs in queue A.
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
					NodeIndicesToCordon: []int{0},
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 1),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{"A": 1, "B": 1},
		},
		"home-away preemption, away jobs first": {
			SchedulingConfig: func() configuration.SchedulingConfig {
				config := testfixtures.TestSchedulingConfig()
				config.DefaultPriorityClassName = "armada-preemptible"
				config.WellKnownNodeTypes = []configuration.WellKnownNodeType{
					{
						Name:   "gpu",
						Taints: []v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}},
					},
				}
				return config
			}(),
			Nodes: testfixtures.TestNodeFactory.AddTaints(
				testfixtures.N8GpuNodes(2, []int32{29000, 30000}),
				[]v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}},
			),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": func() []*jobdb.Job {
							var jobs []*jobdb.Job
							for i := 0; i < 96; i++ {
								jobId := util.ULID()
								jobs = append(jobs, testfixtures.TestJob("A", jobId, "armada-preemptible-away", testfixtures.Test1Cpu4GiPodReqs("A", jobId, 30000)))
							}
							return jobs
						}(),
					},
					ExpectedScheduledIndices: map[string][]int{"A": testfixtures.IntRange(0, 95)},
					ExpectedPreemptedIndices: nil,
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": func() []*jobdb.Job {
							var jobs []*jobdb.Job
							for i := 0; i < 12; i++ {
								jobId := util.ULID()
								req := testfixtures.Test1GpuPodReqs("B", jobId, 30000)
								req.Tolerations = append(req.Tolerations, v1.Toleration{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule})
								jobs = append(jobs, testfixtures.TestJob("B", jobId, "armada-preemptible", req))
							}
							return jobs
						}(),
					},
					ExpectedScheduledIndices: map[string][]int{"B": testfixtures.IntRange(0, 11)},
					ExpectedPreemptedIndices: map[string]map[int][]int{"A": {0: testfixtures.IntRange(32, 95)}},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"home-away preemption, home jobs first": {
			SchedulingConfig: func() configuration.SchedulingConfig {
				config := testfixtures.TestSchedulingConfig()
				config.PriorityClasses = map[string]types.PriorityClass{
					"armada-preemptible-away": {
						Priority:    30000,
						Preemptible: true,

						AwayNodeTypes: []types.AwayNodeType{{Priority: 29000, WellKnownNodeTypeName: "gpu"}},
					},
					"armada-preemptible": {
						Priority:    30000,
						Preemptible: true,
					},
				}
				config.DefaultPriorityClassName = "armada-preemptible"
				return config
			}(),
			Nodes: testfixtures.TestNodeFactory.AddTaints(
				testfixtures.N8GpuNodes(2, []int32{29000, 30000}),
				[]v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}},
			),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": func() []*jobdb.Job {
							var jobs []*jobdb.Job
							for i := 0; i < 12; i++ {
								jobId := util.ULID()
								req := testfixtures.Test1GpuPodReqs("B", jobId, 30000)
								req.Tolerations = append(req.Tolerations, v1.Toleration{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule})
								jobs = append(jobs, testfixtures.TestJob("B", jobId, "armada-preemptible", req))
							}
							return jobs
						}(),
					},
					ExpectedScheduledIndices: map[string][]int{"B": testfixtures.IntRange(0, 11)},
					ExpectedPreemptedIndices: nil,
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": func() []*jobdb.Job {
							var jobs []*jobdb.Job
							for i := 0; i < 96; i++ {
								jobId := util.ULID()
								jobs = append(jobs, testfixtures.TestJob("A", jobId, "armada-preemptible-away", testfixtures.Test1Cpu4GiPodReqs("A", jobId, 30000)))
							}
							return jobs
						}(),
					},
					ExpectedScheduledIndices: map[string][]int{"A": testfixtures.IntRange(0, 31)},
					ExpectedPreemptedIndices: nil,
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"home-away preemption, mixed nodes, away jobs first": {
			SchedulingConfig: func() configuration.SchedulingConfig {
				config := testfixtures.TestSchedulingConfig()
				config.PriorityClasses = map[string]types.PriorityClass{
					"armada-preemptible-away": {
						Priority:    30000,
						Preemptible: true,

						AwayNodeTypes: []types.AwayNodeType{{Priority: 29000, WellKnownNodeTypeName: "gpu"}},
					},
					"armada-preemptible": {
						Priority:    30000,
						Preemptible: true,
					},
				}
				config.DefaultPriorityClassName = "armada-preemptible"
				config.WellKnownNodeTypes = []configuration.WellKnownNodeType{
					{
						Name:   "gpu",
						Taints: []v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}},
					},
				}
				return config
			}(),
			Nodes: func() []*internaltypes.Node {
				priorities := []int32{29000, 30000}
				gpuNodes := testfixtures.TestNodeFactory.AddTaints(
					testfixtures.N8GpuNodes(1, priorities),
					[]v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}},
				)
				return append(testfixtures.N32CpuNodes(1, priorities), gpuNodes...)
			}(),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": func() []*jobdb.Job {
							var jobs []*jobdb.Job
							for i := 0; i < 96; i++ {
								jobId := util.ULID()
								jobs = append(jobs, testfixtures.TestJob("A", jobId, "armada-preemptible-away", testfixtures.Test1Cpu4GiPodReqs("A", jobId, 30000)))
							}
							return jobs
						}(),
					},
					ExpectedScheduledIndices: map[string][]int{"A": testfixtures.IntRange(0, 95)},
					ExpectedPreemptedIndices: nil,
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": func() []*jobdb.Job {
							var jobs []*jobdb.Job
							for i := 0; i < 12; i++ {
								jobId := util.ULID()
								req := testfixtures.Test1GpuPodReqs("B", jobId, 30000)
								req.Tolerations = append(req.Tolerations, v1.Toleration{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule})
								jobs = append(jobs, testfixtures.TestJob("B", jobId, "armada-preemptible", req))
							}
							return jobs
						}(),
					},
					ExpectedScheduledIndices: map[string][]int{"B": testfixtures.IntRange(0, 7)},
					ExpectedPreemptedIndices: map[string]map[int][]int{"A": {0: testfixtures.IntRange(32, 95)}},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"home-away preemption, mixed nodes, home jobs first": {
			SchedulingConfig: func() configuration.SchedulingConfig {
				config := testfixtures.TestSchedulingConfig()
				config.PriorityClasses = map[string]types.PriorityClass{
					"armada-preemptible-away": {
						Priority:    30000,
						Preemptible: true,

						AwayNodeTypes: []types.AwayNodeType{{Priority: 29000, WellKnownNodeTypeName: "gpu"}},
					},
					"armada-preemptible": {
						Priority:    30000,
						Preemptible: true,
					},
				}
				config.DefaultPriorityClassName = "armada-preemptible"
				return config
			}(),
			Nodes: func() []*internaltypes.Node {
				priorities := []int32{29000, 30000}
				gpuNodes := testfixtures.TestNodeFactory.AddTaints(
					testfixtures.N8GpuNodes(1, priorities),
					[]v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}},
				)
				return append(testfixtures.N32CpuNodes(1, priorities), gpuNodes...)
			}(),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": func() []*jobdb.Job {
							var jobs []*jobdb.Job
							for i := 0; i < 12; i++ {
								jobId := util.ULID()
								req := testfixtures.Test1GpuPodReqs("B", jobId, 30000)
								req.Tolerations = append(req.Tolerations, v1.Toleration{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule})
								jobs = append(jobs, testfixtures.TestJob("B", jobId, "armada-preemptible", req))
							}
							return jobs
						}(),
					},
					ExpectedScheduledIndices: map[string][]int{"B": testfixtures.IntRange(0, 7)},
					ExpectedPreemptedIndices: nil,
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": func() []*jobdb.Job {
							var jobs []*jobdb.Job
							for i := 0; i < 96; i++ {
								jobId := util.ULID()
								jobs = append(jobs, testfixtures.TestJob("A", jobId, "armada-preemptible-away", testfixtures.Test1Cpu4GiPodReqs("A", jobId, 30000)))
							}
							return jobs
						}(),
					},
					ExpectedScheduledIndices: map[string][]int{"A": testfixtures.IntRange(0, 31)},
					ExpectedPreemptedIndices: nil,
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"home-away preemption through multiple levels": {
			SchedulingConfig: func() configuration.SchedulingConfig {
				config := testfixtures.TestSchedulingConfig()
				config.ProtectedFractionOfFairShare = 5.0
				config.PriorityClasses = map[string]types.PriorityClass{
					"armada-preemptible-away-lower": {
						Priority:    30000,
						Preemptible: true,

						AwayNodeTypes: []types.AwayNodeType{{Priority: 28000, WellKnownNodeTypeName: "gpu"}},
					},
					"armada-preemptible-away": {
						Priority:    30000,
						Preemptible: true,

						AwayNodeTypes: []types.AwayNodeType{{Priority: 29000, WellKnownNodeTypeName: "gpu"}},
					},
					"armada-preemptible": {
						Priority:    30000,
						Preemptible: true,
					},
				}
				config.DefaultPriorityClassName = "armada-preemptible"
				config.WellKnownNodeTypes = []configuration.WellKnownNodeType{
					{
						Name:   "gpu",
						Taints: []v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}},
					},
				}
				return config
			}(),
			Nodes: testfixtures.TestNodeFactory.AddTaints(
				testfixtures.N32CpuNodes(1, []int32{29000, 28000, 30000}),
				[]v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}},
			),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": func() []*jobdb.Job {
							var jobs []*jobdb.Job
							for i := 0; i < 16; i++ {
								jobId := util.ULID()
								req := testfixtures.Test1Cpu4GiPodReqs("A", jobId, 30000)
								jobs = append(jobs, testfixtures.TestJob("A", jobId, "armada-preemptible-away-lower", req))
							}
							return jobs
						}(),
						"B": func() []*jobdb.Job {
							var jobs []*jobdb.Job
							for i := 0; i < 16; i++ {
								jobId := util.ULID()
								req := testfixtures.Test1Cpu4GiPodReqs("B", jobId, 30000)
								jobs = append(jobs, testfixtures.TestJob("B", jobId, "armada-preemptible-away", req))
							}
							return jobs
						}(),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 15),
						"B": testfixtures.IntRange(0, 15),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"C": func() []*jobdb.Job {
							var jobs []*jobdb.Job
							for i := 0; i < 17; i++ {
								jobId := util.ULID()
								req := testfixtures.Test1Cpu4GiPodReqs("C", jobId, 30000)
								req.Tolerations = append(req.Tolerations, v1.Toleration{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule})
								jobs = append(jobs, testfixtures.TestJob("C", jobId, "armada-preemptible", req))
							}
							return jobs
						}(),
					},
					ExpectedScheduledIndices: map[string][]int{
						"C": testfixtures.IntRange(0, 16),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(0, 15),
						},
						"B": {
							0: testfixtures.IntRange(15, 15),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1, "B": 1, "C": 1,
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			priorities := types.AllowedPriorities(tc.SchedulingConfig.PriorityClasses)

			jobDb := jobdb.NewJobDb(tc.SchedulingConfig.PriorityClasses, tc.SchedulingConfig.DefaultPriorityClassName, stringinterner.New(1024), testfixtures.TestResourceListFactory)
			jobDbTxn := jobDb.WriteTxn()

			// Add all the initial jobs, creating runs for them
			for nodeIdx, jobs := range tc.InitialRunningJobs {
				node := tc.Nodes[nodeIdx]
				for _, job := range jobs {
					err := jobDbTxn.Upsert([]*jobdb.Job{
						job.WithQueued(false).
							WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), node.GetPool(), job.PriorityClass().Priority),
					})
					require.NoError(t, err)
				}
			}

			// Accounting across scheduling rounds.
			roundByJobId := make(map[string]int)
			indexByJobId := make(map[string]int)
			allocatedByQueueAndPriorityClass := make(map[string]map[string]internaltypes.ResourceList)
			nodeIdByJobId := make(map[string]string)

			// Scheduling rate-limiters persist between rounds.
			// We control the rate at which time passes between scheduling rounds.
			schedulingStarted := time.Now()
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

			demandByQueue := map[string]internaltypes.ResourceList{}

			// Run the scheduler.
			cordonedNodes := map[int]bool{}
			ctx := armadacontext.Background()
			for i, round := range tc.Rounds {
				ctx = armadacontext.WithLogField(ctx, "round", i)
				ctx.Infof("starting scheduling round %d", i)

				jobsByNodeId := map[string][]*jobdb.Job{}
				for _, job := range jobDbTxn.GetAll() {
					if job.LatestRun() != nil && !job.LatestRun().InTerminalState() {
						node := job.LatestRun().NodeId()
						jobsByNodeId[node] = append(jobsByNodeId[node], job)
					}
				}

				nodeDb, err := NewNodeDb(tc.SchedulingConfig, stringinterner.New(1024))
				require.NoError(t, err)
				nodeDbTxn := nodeDb.Txn(true)
				for _, node := range tc.Nodes {
					err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(nodeDbTxn, jobsByNodeId[node.GetId()], node.DeepCopyNilKeys())
					require.NoError(t, err)
				}
				nodeDbTxn.Commit()

				// Enqueue jobs that should be considered in this round.
				var queuedJobs []*jobdb.Job
				for queue, jobs := range round.JobsByQueue {
					for j, job := range jobs {
						job = job.WithQueued(true)
						require.Equal(t, queue, job.Queue())
						queuedJobs = append(queuedJobs, job.WithQueued(true))
						roundByJobId[job.Id()] = i
						indexByJobId[job.Id()] = j
						demandByQueue[job.Queue()] = demandByQueue[job.Queue()].Add(job.AllResourceRequirements())
					}
				}
				err = jobDbTxn.Upsert(queuedJobs)
				require.NoError(t, err)

				// Unbind jobs from nodes, to simulate those jobs terminating between rounds.
				for queue, reqIndicesByRoundIndex := range round.IndicesToUnbind {
					for roundIndex, reqIndices := range reqIndicesByRoundIndex {
						for _, reqIndex := range reqIndices {
							job := tc.Rounds[roundIndex].JobsByQueue[queue][reqIndex]
							nodeId := nodeIdByJobId[job.Id()]
							node, err := nodeDb.GetNode(nodeId)
							require.NoError(t, err)
							node, err = nodeDb.UnbindJobFromNode(job, node)
							require.NoError(t, err)
							err = nodeDb.Upsert(node)
							require.NoError(t, err)
							demandByQueue[job.Queue()] = demandByQueue[job.Queue()].Subtract(job.AllResourceRequirements())
						}
					}
				}

				// Cordon nodes.
				for _, idx := range round.NodeIndicesToCordon {
					cordonedNodes[idx] = true
				}
				for idx, isCordoned := range cordonedNodes {
					if isCordoned {
						node, err := nodeDb.GetNode(tc.Nodes[idx].GetId())
						require.NoError(t, err)
						ctx.Infof("Cordoned node %s", node.GetId())
						taints := append(slices.Clone(node.GetTaints()), internaltypes.UnschedulableTaint())
						node = testNodeWithTaints(node, taints)
						err = nodeDb.Upsert(node)
						require.NoError(t, err)
					}
				}

				// If not provided, set total resources equal to the aggregate over tc.Nodes.
				totalResources := nodeDb.TotalKubernetesResources()

				fairnessCostProvider, err := fairness.NewDominantResourceFairness(
					nodeDb.TotalKubernetesResources(),
					testfixtures.TestPool,
					tc.SchedulingConfig,
				)
				require.NoError(t, err)
				sctx := schedulingcontext.NewSchedulingContext(
					testfixtures.TestPool,
					fairnessCostProvider,
					limiter,
					totalResources,
				)
				sctx.Started = schedulingStarted.Add(time.Duration(i) * schedulingInterval)

				for queue, priorityFactor := range tc.PriorityFactorByQueue {
					weight := 1 / priorityFactor
					queueDemand := demandByQueue[queue]
					err := sctx.AddQueueSchedulingContext(
						queue,
						weight,
						weight,
						allocatedByQueueAndPriorityClass[queue],
						queueDemand,
						queueDemand,
						internaltypes.ResourceList{},
						limiterByQueue[queue],
					)
					require.NoError(t, err)
				}
				constraints := schedulerconstraints.NewSchedulingConstraints(
					"pool",
					totalResources,
					tc.SchedulingConfig,
					armadaslices.Map(
						maps.Keys(tc.PriorityFactorByQueue),
						func(qn string) *api.Queue { return &api.Queue{Name: qn} },
					))
				sctx.UpdateFairShares()

				tc.SchedulingConfig.EnablePreferLargeJobOrdering = true

				sch := NewPreemptingQueueScheduler(
					sctx,
					constraints,
					testfixtures.TestEmptyFloatingResources,
					tc.SchedulingConfig,
					jobDbTxn,
					nodeDb,
					round.OptimiserEnabled,
					clock.RealClock{},
				)
				result, err := sch.Schedule(ctx)
				require.NoError(t, err)

				// Test resource accounting.
				for _, jctx := range result.PreemptedJobs {
					job := jctx.Job
					m := allocatedByQueueAndPriorityClass[job.Queue()]
					if m == nil {
						m = make(map[string]internaltypes.ResourceList)
						allocatedByQueueAndPriorityClass[job.Queue()] = m
					}
					m[job.PriorityClassName()] = m[job.PriorityClassName()].Subtract(job.AllResourceRequirements())
				}
				for _, jctx := range result.ScheduledJobs {
					job := jctx.Job
					m := allocatedByQueueAndPriorityClass[job.Queue()]
					if m == nil {
						m = make(map[string]internaltypes.ResourceList)
						allocatedByQueueAndPriorityClass[job.Queue()] = m
					}
					m[job.PriorityClassName()] = m[job.PriorityClassName()].Add(job.AllResourceRequirements())
				}
				for queue, qctx := range sctx.QueueSchedulingContexts {
					m := allocatedByQueueAndPriorityClass[queue]
					assert.Equal(t, internaltypes.RlMapRemoveZeros(m), internaltypes.RlMapRemoveZeros(qctx.AllocatedByPriorityClass))
				}

				// Test that jobs are mapped to nodes correctly.
				for _, jctx := range result.PreemptedJobs {
					job := jctx.Job
					nodeId := jctx.AssignedNode.GetId()
					assert.NotEmpty(t, nodeId)

					// Check that preempted jobs are preempted from the node they were previously scheduled onto.
					expectedNodeId := nodeIdByJobId[job.Id()]
					assert.Equal(t, expectedNodeId, nodeId, "job %s preempted from unexpected node", job.Id())
				}
				for _, jctx := range result.ScheduledJobs {
					job := jctx.Job
					nodeId := jctx.PodSchedulingContext.NodeId
					assert.NotEmpty(t, nodeId)

					node, err := nodeDb.GetNode(nodeId)
					require.NoError(t, err)
					assert.NotEmpty(t, node)

					// Check that the job can actually go onto this node.
					matches, reason, err := nodedb.StaticJobRequirementsMet(node, jctx)
					require.NoError(t, err)
					assert.Empty(t, reason)
					assert.True(t, matches)

					// Check that scheduled jobs are consistently assigned to the same node.
					// (We don't allow moving jobs between nodes.)
					if expectedNodeId, ok := nodeIdByJobId[job.Id()]; ok {
						assert.Equal(t, expectedNodeId, nodeId, "job %s scheduled onto unexpected node", job.Id())
					} else {
						nodeIdByJobId[job.Id()] = nodeId
					}
				}

				// Expected scheduled jobs.
				jobIdsByQueue := jobIdsByQueueFromJobContexts(result.ScheduledJobs)
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
				jobIdsByQueue = jobIdsByQueueFromJobContexts(result.PreemptedJobs)
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
				it, err := nodedb.NewNodesIterator(nodeDb.Txn(false))
				require.NoError(t, err)
				for node := it.NextNode(); node != nil; node = it.NextNode() {
					for _, p := range priorities {
						for _, r := range node.AllocatableByPriority[p].GetAll() {
							assert.False(t, r.IsNegative(), "resource oversubscribed by %s on node %s", r.String(), node.GetId())
						}
					}
				}

				err = jobDbTxn.BatchDelete(armadaslices.Map(queuedJobs, func(job *jobdb.Job) string { return job.Id() }))
				require.NoError(t, err)

				var preemptedJobs []*jobdb.Job
				for _, jctx := range result.PreemptedJobs {
					job := jctx.Job
					preemptedJobs = append(
						preemptedJobs,
						job.
							WithUpdatedRun(job.LatestRun().WithFailed(true)).
							WithQueued(false).
							WithFailed(true),
					)
				}
				err = jobDbTxn.Upsert(preemptedJobs)
				require.NoError(t, err)

				// Jobs may arrive out of order here; sort them, so that runs
				// are created in the right order (this influences the order in
				// which jobs are preempted).
				slices.SortFunc(
					result.ScheduledJobs,
					func(a, b *schedulingcontext.JobSchedulingContext) int {
						if a.Job.SubmitTime().Before(b.Job.SubmitTime()) {
							return -1
						} else if b.Job.SubmitTime().Before(a.Job.SubmitTime()) {
							return 1
						} else {
							return 0
						}
					},
				)
				var scheduledJobs []*jobdb.Job
				for _, jctx := range result.ScheduledJobs {
					job := jctx.Job
					jobId := job.Id()
					node, err := nodeDb.GetNode(jctx.PodSchedulingContext.NodeId)
					require.NotNil(t, node)
					require.NoError(t, err)
					priority, ok := nodeDb.GetScheduledAtPriority(jobId)
					require.True(t, ok)
					scheduledJobs = append(
						scheduledJobs,
						job.WithQueuedVersion(job.QueuedVersion()+1).
							WithQueued(false).
							WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), node.GetPool(), priority),
					)
				}
				err = jobDbTxn.Upsert(scheduledJobs)
				require.NoError(t, err)
			}
		})
	}
}

func jobIdsByQueueFromJobContexts(jctxs []*schedulingcontext.JobSchedulingContext) map[string][]string {
	rv := make(map[string][]string)
	for _, jctx := range jctxs {
		job := jctx.Job
		rv[job.Queue()] = append(rv[job.Queue()], job.Id())
	}
	return rv
}

func BenchmarkPreemptingQueueScheduler(b *testing.B) {
	tests := map[string]struct {
		SchedulingConfig  configuration.SchedulingConfig
		Nodes             []*internaltypes.Node
		JobFunc           func(queue string, priorityClassName string, n int) []*jobdb.Job
		NumQueues         int
		NumJobsPerQueue   int
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
			ctx := armadacontext.New(context.Background(), logging.NullLogger)
			jobsByQueue := make(map[string][]*jobdb.Job)
			priorityFactorByQueue := make(map[string]float64)
			for i := 0; i < tc.NumQueues; i++ {
				queue := fmt.Sprintf("%d", i)
				jobsByQueue[queue] = tc.JobFunc(queue, testfixtures.PriorityClass0, tc.NumJobsPerQueue)
				priorityFactorByQueue[queue] = float64(rand.Intn(tc.MaxPriorityFactor-tc.MinPriorityFactor+1) + tc.MinPriorityFactor)
			}

			nodeDb, err := NewNodeDb(tc.SchedulingConfig, stringinterner.New(1024))
			require.NoError(b, err)
			txn := nodeDb.Txn(true)
			for _, node := range tc.Nodes {
				err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node.DeepCopyNilKeys())
				require.NoError(b, err)
			}
			txn.Commit()

			jobDb := jobdb.NewJobDb(tc.SchedulingConfig.PriorityClasses, tc.SchedulingConfig.DefaultPriorityClassName, stringinterner.New(1024), testfixtures.TestResourceListFactory)
			jobDbTxn := jobDb.WriteTxn()
			var queuedJobs []*jobdb.Job
			for _, jobs := range jobsByQueue {
				for _, job := range jobs {
					queuedJobs = append(queuedJobs, job.WithQueued(true))
				}
			}
			err = jobDbTxn.Upsert(queuedJobs)
			require.NoError(b, err)

			limiter := rate.NewLimiter(
				rate.Limit(tc.SchedulingConfig.MaximumSchedulingRate),
				tc.SchedulingConfig.MaximumSchedulingBurst,
			)
			limiterByQueue := make(map[string]*rate.Limiter)
			for queue := range priorityFactorByQueue {
				limiterByQueue[queue] = rate.NewLimiter(
					rate.Limit(tc.SchedulingConfig.MaximumPerQueueSchedulingRate),
					tc.SchedulingConfig.MaximumPerQueueSchedulingBurst,
				)
			}

			fairnessCostProvider, err := fairness.NewDominantResourceFairness(
				nodeDb.TotalKubernetesResources(),
				testfixtures.TestPool,
				tc.SchedulingConfig,
			)
			require.NoError(b, err)
			sctx := schedulingcontext.NewSchedulingContext(
				testfixtures.TestPool,
				fairnessCostProvider,
				limiter,
				nodeDb.TotalKubernetesResources(),
			)
			for queue, priorityFactor := range priorityFactorByQueue {
				weight := 1 / priorityFactor
				err := sctx.AddQueueSchedulingContext(queue, weight, weight, make(map[string]internaltypes.ResourceList),
					internaltypes.ResourceList{}, internaltypes.ResourceList{}, internaltypes.ResourceList{}, limiterByQueue[queue])
				require.NoError(b, err)
			}
			constraints := schedulerconstraints.NewSchedulingConstraints(
				testfixtures.TestPool,
				nodeDb.TotalKubernetesResources(),
				tc.SchedulingConfig,
				armadaslices.Map(
					maps.Keys(priorityFactorByQueue),
					func(qn string) *api.Queue { return &api.Queue{Name: qn} },
				),
			)

			tc.SchedulingConfig.EnablePreferLargeJobOrdering = true

			sch := NewPreemptingQueueScheduler(
				sctx,
				constraints,
				testfixtures.TestEmptyFloatingResources,
				tc.SchedulingConfig,
				jobDbTxn,
				nodeDb,
				false,
				clock.RealClock{},
			)
			result, err := sch.Schedule(ctx)
			require.NoError(b, err)
			require.Equal(b, 0, len(result.PreemptedJobs))

			scheduledJobs := make(map[string]bool)
			for _, jctx := range result.ScheduledJobs {
				scheduledJobs[jctx.JobId] = true
			}
			err = jobDbTxn.BatchDelete(
				armadaslices.Map(
					result.ScheduledJobs,
					func(jctx *schedulingcontext.JobSchedulingContext) string {
						return jctx.JobId
					},
				),
			)
			require.NoError(b, err)

			jobsByNodeId := make(map[string][]*jobdb.Job)
			for _, jctx := range result.ScheduledJobs {
				nodeId := jctx.PodSchedulingContext.NodeId
				jobsByNodeId[nodeId] = append(jobsByNodeId[nodeId], jctx.Job)
			}
			nodeDb, err = NewNodeDb(tc.SchedulingConfig, stringinterner.New(1024))
			require.NoError(b, err)
			txn = nodeDb.Txn(true)
			for _, node := range tc.Nodes {
				err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, jobsByNodeId[node.GetId()], node.DeepCopyNilKeys())
				require.NoError(b, err)
			}
			txn.Commit()

			allocatedByQueueAndPriorityClass := sctx.AllocatedByQueueAndPriority()

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				sctx := schedulingcontext.NewSchedulingContext(
					"pool",
					fairnessCostProvider,
					limiter,
					nodeDb.TotalKubernetesResources(),
				)
				for queue, priorityFactor := range priorityFactorByQueue {
					weight := 1 / priorityFactor
					err := sctx.AddQueueSchedulingContext(queue, weight, weight, allocatedByQueueAndPriorityClass[queue],
						internaltypes.ResourceList{}, internaltypes.ResourceList{}, internaltypes.ResourceList{}, limiterByQueue[queue])
					require.NoError(b, err)
				}

				tc.SchedulingConfig.EnablePreferLargeJobOrdering = true

				sch := NewPreemptingQueueScheduler(
					sctx,
					constraints,
					testfixtures.TestEmptyFloatingResources,
					tc.SchedulingConfig,
					jobDbTxn,
					nodeDb,
					false,
					clock.RealClock{},
				)
				result, err := sch.Schedule(ctx)
				require.NoError(b, err)

				// We expect the system to be in steady-state, i.e., no preempted/scheduled jobs.
				require.Equal(b, 0, len(result.PreemptedJobs))
				require.Equal(b, 0, len(result.ScheduledJobs))
			}
		})
	}
}

func TestPreemptingQueueSchedulerTimeouts(t *testing.T) {
	t.Run("soft timeout: schedules evicted jobs, blocks new jobs", func(t *testing.T) {
		// Setup: Queue A has 32 running jobs consuming all resources on 1 node.
		// Queue B submits 32 new jobs. Fairness requires evicting some of A's jobs.
		// With soft timeout, only A's evicted jobs should be re-scheduled, not B's new jobs.
		config := testfixtures.TestSchedulingConfig()
		stringInterner := stringinterner.New(1024)
		nodes := testfixtures.N32CpuNodes(1, testfixtures.TestPriorities)
		node := nodes[0]

		// Queue A: 32 jobs already running on the node
		runningJobs := testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32)
		jobDb := jobdb.NewJobDb(config.PriorityClasses, config.DefaultPriorityClassName, stringInterner, testfixtures.TestResourceListFactory)
		jobDbTxn := jobDb.WriteTxn()
		for _, job := range runningJobs {
			err := jobDbTxn.Upsert([]*jobdb.Job{
				job.WithQueued(false).WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), node.GetPool(), job.PriorityClass().Priority),
			})
			require.NoError(t, err)
		}

		// Queue B: 32 new queued jobs
		newJobs := testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 32)
		require.NoError(t, jobDbTxn.Upsert(newJobs))

		// Create nodeDb with running jobs placed on node
		nodeDb, err := NewNodeDb(config, stringInterner)
		require.NoError(t, err)
		txn := nodeDb.Txn(true)
		runningJobsFromDb := make([]*jobdb.Job, 0, 32)
		for _, job := range jobDbTxn.GetAll() {
			if job.LatestRun() != nil {
				runningJobsFromDb = append(runningJobsFromDb, job)
			}
		}
		require.NoError(t, nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, runningJobsFromDb, node.DeepCopyNilKeys()))
		txn.Commit()

		totalResources := nodeDb.TotalKubernetesResources()
		fairnessCostProvider, err := fairness.NewDominantResourceFairness(totalResources, testfixtures.TestPool, config)
		require.NoError(t, err)

		sctx := schedulingcontext.NewSchedulingContext(
			testfixtures.TestPool,
			fairnessCostProvider,
			rate.NewLimiter(rate.Limit(config.MaximumSchedulingRate), config.MaximumSchedulingBurst),
			totalResources,
		)

		demandA := testfixtures.TestResourceListFactory.MakeAllZero()
		for _, job := range runningJobs {
			demandA = demandA.Add(job.AllResourceRequirements())
		}
		demandB := testfixtures.TestResourceListFactory.MakeAllZero()
		for _, job := range newJobs {
			demandB = demandB.Add(job.AllResourceRequirements())
		}

		// Queue A has initial allocation (jobs already running)
		initialAllocatedA := map[string]internaltypes.ResourceList{
			testfixtures.PriorityClass0: demandA,
		}
		require.NoError(t, sctx.AddQueueSchedulingContext(
			"A", 1.0, 1.0, initialAllocatedA, demandA, demandA, internaltypes.ResourceList{},
			rate.NewLimiter(rate.Limit(config.MaximumPerQueueSchedulingRate), config.MaximumPerQueueSchedulingBurst),
		))
		require.NoError(t, sctx.AddQueueSchedulingContext(
			"B", 1.0, 1.0, nil, demandB, demandB, internaltypes.ResourceList{},
			rate.NewLimiter(rate.Limit(config.MaximumPerQueueSchedulingRate), config.MaximumPerQueueSchedulingBurst),
		))

		constraints := schedulerconstraints.NewSchedulingConstraints(
			testfixtures.TestPool, totalResources, config,
			[]*api.Queue{{Name: "A", PriorityFactor: 1.0}, {Name: "B", PriorityFactor: 1.0}},
		)

		sch := NewPreemptingQueueScheduler(
			sctx, constraints, testfixtures.TestEmptyFloatingResources,
			config, jobDbTxn, nodeDb, false, clock.RealClock{},
		)

		result, err := sch.Schedule(armadacontext.Background())
		require.NoError(t, err)
		require.NotNil(t, result)

		// With soft timeout: B's new queued jobs should NOT be scheduled.
		// A's evicted jobs get re-scheduled but don't appear in ScheduledJobs
		// (ScheduledJobs only contains newly scheduled queued jobs, not re-scheduled evicted jobs).
		// Since A had no queued jobs and B's jobs were blocked by soft timeout, ScheduledJobs should be empty.
		assert.Empty(t, result.ScheduledJobs, "no new queued jobs should be scheduled after soft timeout")
	})

	t.Run("hard timeout: returns error", func(t *testing.T) {
		config := testfixtures.TestSchedulingConfig()
		stringInterner := stringinterner.New(1024)

		nodeDb, err := NewNodeDb(config, stringInterner)
		require.NoError(t, err)
		txn := nodeDb.Txn(true)
		for _, node := range testfixtures.N32CpuNodes(1, testfixtures.TestPriorities) {
			require.NoError(t, nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node.DeepCopyNilKeys()))
		}
		txn.Commit()

		totalResources := nodeDb.TotalKubernetesResources()
		fairnessCostProvider, err := fairness.NewDominantResourceFairness(totalResources, testfixtures.TestPool, config)
		require.NoError(t, err)

		jobs := testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 10)
		jobDb := jobdb.NewJobDb(config.PriorityClasses, config.DefaultPriorityClassName, stringInterner, testfixtures.TestResourceListFactory)
		jobDbTxn := jobDb.WriteTxn()
		require.NoError(t, jobDbTxn.Upsert(jobs))

		sctx := schedulingcontext.NewSchedulingContext(
			testfixtures.TestPool,
			fairnessCostProvider,
			rate.NewLimiter(rate.Limit(config.MaximumSchedulingRate), config.MaximumSchedulingBurst),
			totalResources,
		)
		demand := testfixtures.TestResourceListFactory.MakeAllZero()
		for _, job := range jobs {
			demand = demand.Add(job.AllResourceRequirements())
		}
		require.NoError(t, sctx.AddQueueSchedulingContext(
			"A", 1.0, 1.0, nil, demand, demand, internaltypes.ResourceList{},
			rate.NewLimiter(rate.Limit(config.MaximumPerQueueSchedulingRate), config.MaximumPerQueueSchedulingBurst),
		))

		constraints := schedulerconstraints.NewSchedulingConstraints(
			testfixtures.TestPool, totalResources, config,
			[]*api.Queue{{Name: "A", PriorityFactor: 1.0}},
		)

		sch := NewPreemptingQueueScheduler(
			sctx, constraints, testfixtures.TestEmptyFloatingResources,
			config, jobDbTxn, nodeDb, false, clock.RealClock{},
		)

		ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
		cancel()

		result, err := sch.Schedule(ctx)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func testNodeWithTaints(node *internaltypes.Node, taints []v1.Taint) *internaltypes.Node {
	return internaltypes.CreateNode(
		node.GetId(),
		node.GetNodeType(),
		node.GetIndex(),
		node.GetExecutor(),
		node.GetName(),
		node.GetPool(),
		node.GetReportingNodeType(),
		taints,
		node.GetLabels(),
		false,
		node.GetTotalResources(),
		node.GetAllocatableResources(),
		node.AllocatableByPriority,
		node.AllocatedByQueue,
		node.AllocatedByJobId,
		node.EvictedJobRunIds,
		node.Keys,
	)
}
