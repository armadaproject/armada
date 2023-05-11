package scheduler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestGangScheduler(t *testing.T) {
	tests := map[string]struct {
		SchedulingConfig configuration.SchedulingConfig
		// Minimum job size.
		MinimumJobSize map[string]resource.Quantity
		// Nodes to be considered by the scheduler.
		Nodes []*schedulerobjects.Node
		// Total resources across all clusters.
		// Set to the total resources across all nodes if not provided.
		TotalResources schedulerobjects.ResourceList
		// Gangs to try scheduling.
		Gangs [][]*jobdb.Job
		// Indices of gangs expected to be scheduled.
		ExpectedScheduledIndices []int
	}{
		"simple success": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
			},
			ExpectedScheduledIndices: testfixtures.IntRange(0, 0),
		},
		"simple failure": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 33),
			},
			ExpectedScheduledIndices: nil,
		},
		"one success and one failure": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1),
			},
			ExpectedScheduledIndices: testfixtures.IntRange(0, 0),
		},
		"multiple nodes": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 64),
			},
			ExpectedScheduledIndices: testfixtures.IntRange(0, 0),
		},
		"MaximalClusterFractionToSchedule": {
			SchedulingConfig: testfixtures.WithRoundLimitsConfig(
				map[string]float64{"cpu": 0.5},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 8),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 16),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 8),
			},
			ExpectedScheduledIndices: []int{0, 2},
		},
		"per priority per-queue limits": {
			SchedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[int32]map[string]float64{
					0: {"cpu": 1.0},
					1: {"cpu": 15.0 / 32.0},
					2: {"cpu": 10.0 / 32.0},
					3: {"cpu": 3.0 / 32.0},
				},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass3, 4),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass3, 3),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass2, 8),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass2, 7),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass1, 6),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass1, 5),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 18),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 17),
			},
			ExpectedScheduledIndices: []int{1, 3, 5, 7},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := CreateNodeDb(tc.Nodes)
			require.NoError(t, err)
			if tc.TotalResources.Resources == nil {
				// Default to NodeDb total.
				tc.TotalResources = nodeDb.TotalResources()
			}
			priorityFactorByQueue := make(map[string]float64)
			for _, jobs := range tc.Gangs {
				for _, job := range jobs {
					priorityFactorByQueue[job.GetQueue()] = 1
				}
			}
			sctx := schedulercontext.NewSchedulingContext(
				"executor",
				"pool",
				tc.SchedulingConfig.Preemption.PriorityClasses,
				tc.SchedulingConfig.Preemption.DefaultPriorityClass,
				tc.SchedulingConfig.ResourceScarcity,
				priorityFactorByQueue,
				tc.TotalResources,
				nil,
			)
			constraints := schedulerconstraints.SchedulingConstraintsFromSchedulingConfig(
				schedulerobjects.ResourceList{Resources: tc.MinimumJobSize},
				tc.SchedulingConfig,
			)
			sch, err := NewGangScheduler(sctx, constraints, nodeDb)
			require.NoError(t, err)

			var actualScheduledIndices []int
			for i, gang := range tc.Gangs {
				jctxs := jobSchedulingContextsFromJobs(gang, "", testfixtures.TestPriorityClasses)
				gctx := schedulercontext.NewGangSchedulingContext(jctxs)
				ok, reason, err := sch.Schedule(context.Background(), gctx)
				require.NoError(t, err)
				if ok {
					require.Empty(t, reason)
					actualScheduledIndices = append(actualScheduledIndices, i)
				} else {
					require.NotEmpty(t, reason)
				}
			}
			assert.Equal(t, tc.ExpectedScheduledIndices, actualScheduledIndices)
		})
	}
}
