package scheduling

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	armadaconfiguration "github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/api"
)

func TestQueueScheduler(t *testing.T) {
	schedulingConfigWithPreferLargeJobDisabled := testfixtures.TestSchedulingConfig()
	schedulingConfigWithPreferLargeJobDisabled.EnablePreferLargeJobOrdering = false
	tests := map[string]struct {
		SchedulingConfig configuration.SchedulingConfig
		// Queues
		Queues []*api.Queue
		// Initial resource usage for all queues.
		InitialAllocatedByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList
		// Nodes to be considered by the scheduler.
		Nodes []*internaltypes.Node
		// Jobs to try scheduling.
		Jobs []*jobdb.Job
		// Indices of jobs expected to be scheduled.
		ExpectedScheduledIndices []int
		// Indices of jobs the scheduler should never have tried to schedule.
		ExpectedNeverAttemptedIndices []int
	}{
		"simple success": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			Nodes:                    testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
			ExpectedScheduledIndices: testfixtures.IntRange(0, 31),
		},
		"simple failure": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			Nodes:                    testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 33),
			ExpectedScheduledIndices: testfixtures.IntRange(0, 31),
		},
		"multiple nodes": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			Nodes:                    testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Jobs:                     testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 64),
			ExpectedScheduledIndices: testfixtures.IntRange(0, 63),
		},
		"preempt lower-priority jobs": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1), testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 1)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: testfixtures.IntRange(0, 1),
		},
		"no preemption of higher-priority jobs": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass1, 1), testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: testfixtures.IntRange(0, 0),
		},
		"unschedulable jobs do not block schedulable jobs": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 10),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
			),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0, 11},
		},
		"MaximumSchedulingBurst": {
			SchedulingConfig: testfixtures.WithGlobalSchedulingRateLimiterConfig(10, 2, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 10),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 4),
			),
			Queues:                        testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices:      []int{0, 11},
			ExpectedNeverAttemptedIndices: []int{13, 14},
		},
		"MaximumPerQueueSchedulingBurst": {
			SchedulingConfig: testfixtures.WithPerQueueSchedulingLimiterConfig(10, 2, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 10),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 3),
				testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 1),
			),
			Queues:                        []*api.Queue{{Name: "A", PriorityFactor: 1.0}, {Name: "B", PriorityFactor: 1.0}},
			ExpectedScheduledIndices:      []int{0, 11, 14},
			ExpectedNeverAttemptedIndices: []int{13},
		},
		"MaximumSchedulingBurst is not exceeded by gangs": {
			SchedulingConfig: testfixtures.WithGlobalSchedulingRateLimiterConfig(10, 2, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1),
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 2),
				),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 2),
			),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0, 4},
		},
		"MaximumPerQueueSchedulingBurst is not exceeded by gangs": {
			SchedulingConfig: testfixtures.WithPerQueueSchedulingLimiterConfig(10, 2, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1),
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 2),
				),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 2),
			),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0, 4},
		},
		"MaximumResourceFractionToSchedule": {
			SchedulingConfig: testfixtures.WithRoundLimitsConfig(
				map[string]float64{"cpu": 0.5},
				testfixtures.TestSchedulingConfig(),
			),
			Queues:                        testfixtures.SingleQueuePriorityOne("A"),
			Nodes:                         testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                          testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
			ExpectedScheduledIndices:      testfixtures.IntRange(0, 16),
			ExpectedNeverAttemptedIndices: testfixtures.IntRange(17, 31),
		},
		"PerPriorityLimits": {
			SchedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[string]map[string]float64{
					testfixtures.PriorityClass0: {"cpu": 1.0 / 32.0},
					testfixtures.PriorityClass1: {"cpu": 2.0 / 32.0},
					testfixtures.PriorityClass2: {"cpu": 3.0 / 32.0},
					testfixtures.PriorityClass3: {"cpu": 4.0 / 32.0},
				},
				testfixtures.TestSchedulingConfig(),
			),
			Queues: testfixtures.SingleQueuePriorityOne("A"),
			Nodes:  testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 2),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 2),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 3),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2, 3),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2, 3),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass3, 4),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass3, 4),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
			),
			ExpectedScheduledIndices: armadaslices.Concatenate(
				testfixtures.IntRange(0, 0),
				testfixtures.IntRange(3, 4),
				testfixtures.IntRange(8, 10),
				testfixtures.IntRange(14, 17),
			),
		},
		"per queue, resource class, and pool cpu limit": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Queues: []*api.Queue{
				{
					Name:           "A",
					PriorityFactor: 1.0,
					ResourceLimitsByPriorityClassName: map[string]*api.PriorityClassResourceLimits{
						testfixtures.PriorityClass0: {
							MaximumResourceFractionByPool: map[string]*api.PriorityClassPoolResourceLimits{
								"pool": {
									MaximumResourceFraction: map[string]float64{"cpu": 0.5, "memory": 1.0},
								},
							},
						},
					},
				},
			},
			Nodes:                    testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
			ExpectedScheduledIndices: testfixtures.IntRange(0, 15),
		},
		"should scheduled largest job first - prioritiseLargerJobs enabled": {
			// Only schedule 1 to show which queue went first
			SchedulingConfig:              testfixtures.WithGlobalSchedulingRateLimiterConfig(1, 1, testfixtures.TestSchedulingConfig()),
			Nodes:                         testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                          armadaslices.Concatenate(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 32), testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 32)),
			Queues:                        []*api.Queue{{Name: "A", PriorityFactor: 1.0}, {Name: "B", PriorityFactor: 1.0}},
			ExpectedScheduledIndices:      armadaslices.Concatenate(testfixtures.IntRange(0, 0)),
			ExpectedNeverAttemptedIndices: armadaslices.Concatenate(testfixtures.IntRange(1, 31), testfixtures.IntRange(33, 63)),
		},
		"should scheduled smallest job first - prioritiseLargerJobs disabled": {
			// Only schedule 1 to show which queue went first
			SchedulingConfig:         testfixtures.WithGlobalSchedulingRateLimiterConfig(1, 1, schedulingConfigWithPreferLargeJobDisabled),
			Nodes:                    testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 32), testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 32)),
			Queues:                   []*api.Queue{{Name: "A", PriorityFactor: 1.0}, {Name: "B", PriorityFactor: 1.0}},
			ExpectedScheduledIndices: armadaslices.Concatenate(testfixtures.IntRange(32, 32)),
			// Note queue A is never attempted
			ExpectedNeverAttemptedIndices: armadaslices.Concatenate(testfixtures.IntRange(0, 31), testfixtures.IntRange(34, 63)),
		},
		"fairness two queues": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32), testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 32)),
			Queues:                   []*api.Queue{{Name: "A", PriorityFactor: 1.0}, {Name: "B", PriorityFactor: 1.0}},
			ExpectedScheduledIndices: armadaslices.Concatenate(testfixtures.IntRange(0, 15), testfixtures.IntRange(32, 47)),
		},
		"fairness three queues": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
				testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 32),
				testfixtures.N1Cpu4GiJobs("C", testfixtures.PriorityClass0, 32),
			),
			Queues: []*api.Queue{{Name: "A", PriorityFactor: 1.0}, {Name: "B", PriorityFactor: 1.0}, {Name: "C", PriorityFactor: 1.0}},
			ExpectedScheduledIndices: armadaslices.Concatenate(
				testfixtures.IntRange(0, 10),
				testfixtures.IntRange(32, 42),
				testfixtures.IntRange(64, 73),
			),
		},
		"weighted fairness two queues": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 96),
				testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 96),
			),
			Queues: []*api.Queue{{Name: "A", PriorityFactor: 1.0}, {Name: "B", PriorityFactor: 2.0}},
			ExpectedScheduledIndices: armadaslices.Concatenate(
				testfixtures.IntRange(0, 63),
				testfixtures.IntRange(96, 127),
			),
		},
		"weighted fairness three queues": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 96),
				testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 96),
				testfixtures.N1Cpu4GiJobs("C", testfixtures.PriorityClass0, 96),
			),
			Queues: []*api.Queue{{Name: "A", PriorityFactor: 1.0}, {Name: "B", PriorityFactor: 2.0}, {Name: "C", PriorityFactor: 10.0}},
			ExpectedScheduledIndices: armadaslices.Concatenate(
				testfixtures.IntRange(0, 59),
				testfixtures.IntRange(96, 125),
				testfixtures.IntRange(192, 197),
			),
		},
		"fairness two queues with initial allocation": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
				testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 32),
			),
			Queues: []*api.Queue{{Name: "A", PriorityFactor: 1.0}, {Name: "B", PriorityFactor: 1.0}},
			InitialAllocatedByQueueAndPriorityClass: map[string]map[string]internaltypes.ResourceList{
				"A": {
					testfixtures.PriorityClass0: testfixtures.Cpu("100"),
				},
			},
			ExpectedScheduledIndices: testfixtures.IntRange(32, 63),
		},
		"node with no available capacity": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: testfixtures.WithUsedResourcesNodes(
				0,
				testfixtures.Cpu("32"),
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			),
			Jobs:                     testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: nil,
		},
		"node with some available capacity": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: testfixtures.WithUsedResourcesNodes(
				0,
				testfixtures.Cpu("31"),
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			),
			Jobs:                     testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 2),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0},
		},
		"preempt used resources of lower-priority jobs": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: testfixtures.WithUsedResourcesNodes(
				0,
				testfixtures.Cpu("32"),
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			),
			Jobs:                     testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass1, 1),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0},
		},
		"respect taints": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.NTainted32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1), testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{1},
		},
		"taints and tolerations": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.NTainted32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1), testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{1},
		},
		"Node selector": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: armadaslices.Concatenate(
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				testfixtures.TestNodeFactory.AddLabels(
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					map[string]string{"foo": "foo"},
				),
			),
			Jobs:                     testfixtures.WithNodeSelectorJobs(map[string]string{"foo": "foo"}, testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 2)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0},
		},
		"taints and tolerations (indexed)": {
			SchedulingConfig:         testfixtures.WithIndexedTaintsConfig([]string{"largeJobsOnly"}, testfixtures.TestSchedulingConfig()),
			Nodes:                    testfixtures.NTainted32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1), testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{1},
		},
		"Node selector (indexed)": {
			SchedulingConfig: testfixtures.WithIndexedNodeLabelsConfig([]string{"foo"}, testfixtures.TestSchedulingConfig()),
			Nodes: append(
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				testfixtures.TestNodeFactory.AddLabels(
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					map[string]string{"foo": "foo"},
				)...,
			),
			Jobs:                     testfixtures.WithNodeSelectorJobs(map[string]string{"foo": "foo"}, testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 2)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0},
		},
		"MaxQueueLookback": {
			SchedulingConfig: testfixtures.WithMaxQueueLookbackConfig(3, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 3),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
			),
			Queues:                        testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices:      []int{0},
			ExpectedNeverAttemptedIndices: []int{3, 4},
		},
		"gang success": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Jobs:                     testfixtures.WithGangAnnotationsJobs(testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 2)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0, 1},
		},
		"non-consecutive gang success": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.WithAnnotationsJobs(map[string]string{
					armadaconfiguration.GangIdAnnotation:          "my-gang",
					armadaconfiguration.GangCardinalityAnnotation: "2",
				},
					testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1)),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.WithAnnotationsJobs(map[string]string{
					armadaconfiguration.GangIdAnnotation:          "my-gang",
					armadaconfiguration.GangCardinalityAnnotation: "2",
				},
					testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1)),
			),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0, 1, 2},
		},
		"gang failure": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Jobs:                     testfixtures.WithGangAnnotationsJobs(testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 3)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: nil,
		},
		"non-consecutive gang failure": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.WithAnnotationsJobs(map[string]string{
					armadaconfiguration.GangIdAnnotation:          "my-gang",
					armadaconfiguration.GangCardinalityAnnotation: "2",
				},
					testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1)),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.WithAnnotationsJobs(map[string]string{
					armadaconfiguration.GangIdAnnotation:          "my-gang",
					armadaconfiguration.GangCardinalityAnnotation: "2",
				},
					testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1)),
			),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{1},
		},
		"job priority": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.WithPriorityJobs(10, testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithPriorityJobs(1, testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithPriorityJobs(20, testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1)),
			),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{1},
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
			Jobs: armadaslices.Concatenate(
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
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0, 1},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := NewNodeDb(tc.SchedulingConfig, stringinterner.New(1024))
			require.NoError(t, err)
			txn := nodeDb.Txn(true)
			for _, node := range tc.Nodes {
				err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node.DeepCopyNilKeys())
				require.NoError(t, err)
			}
			txn.Commit()

			totalResources := nodeDb.TotalKubernetesResources()

			queueNameToQueue := map[string]*api.Queue{}
			for _, q := range tc.Queues {
				queueNameToQueue[q.Name] = q
			}

			indexByJobId := make(map[string]int)
			for i, job := range tc.Jobs {
				if _, ok := queueNameToQueue[job.Queue()]; !ok {
					panic(fmt.Sprintf("queue %s does not exist", job.Queue()))
				}
				indexByJobId[job.Id()] = i
			}
			jobRepo := NewInMemoryJobRepository(testfixtures.TestPool)
			jobRepo.EnqueueMany(
				context.JobSchedulingContextsFromJobs(tc.Jobs),
			)

			fairnessCostProvider, err := fairness.NewDominantResourceFairness(
				totalResources,
				testfixtures.TestPool,
				tc.SchedulingConfig,
			)
			require.NoError(t, err)
			sctx := context.NewSchedulingContext(
				"pool",
				fairnessCostProvider,
				rate.NewLimiter(
					rate.Limit(tc.SchedulingConfig.MaximumSchedulingRate),
					tc.SchedulingConfig.MaximumSchedulingBurst,
				),
				totalResources,
			)
			for _, q := range tc.Queues {
				demand := testfixtures.TestResourceListFactory.MakeAllZero()
				for _, job := range tc.Jobs {
					if job.Queue() == q.Name {
						demand = demand.Add(job.AllResourceRequirements())
					}
				}
				weight := 1.0 / float64(q.PriorityFactor)
				err := sctx.AddQueueSchedulingContext(
					q.Name, weight, weight,
					tc.InitialAllocatedByQueueAndPriorityClass[q.Name],
					demand,
					demand,
					internaltypes.ResourceList{},
					rate.NewLimiter(
						rate.Limit(tc.SchedulingConfig.MaximumPerQueueSchedulingRate),
						tc.SchedulingConfig.MaximumPerQueueSchedulingBurst,
					),
				)
				require.NoError(t, err)
			}
			sctx.UpdateFairShares()
			constraints := schedulerconstraints.NewSchedulingConstraints("pool", totalResources, tc.SchedulingConfig, tc.Queues)
			jobIteratorByQueue := make(map[string]JobContextIterator)
			for _, q := range tc.Queues {
				it := jobRepo.GetJobIterator(q.Name)
				jobIteratorByQueue[q.Name] = it
			}
			sch, err := NewQueueScheduler(sctx, constraints, testfixtures.TestEmptyFloatingResources, nodeDb, jobIteratorByQueue, false, false, tc.SchedulingConfig.EnablePreferLargeJobOrdering, tc.SchedulingConfig.MaxQueueLookback)
			require.NoError(t, err)

			result, err := sch.Schedule(armadacontext.Background())
			require.NoError(t, err)

			// Check that the right jobs got scheduled.
			var actualScheduledIndices []int
			for _, jctx := range result.ScheduledJobs {
				actualScheduledIndices = append(
					actualScheduledIndices,
					indexByJobId[jctx.JobId],
				)
			}
			slices.Sort(actualScheduledIndices)
			assert.Equal(t, tc.ExpectedScheduledIndices, actualScheduledIndices, "actual scheduled indices does not match expected")

			// Check that the right job scheduling contexts were created.
			expectedScheduledIndicesByQueue := armadaslices.GroupByFunc(
				tc.ExpectedScheduledIndices,
				func(i int) string {
					return tc.Jobs[i].Queue()
				},
			)
			expectedSuccessfulOrNotAttemptedIndices := armadaslices.MapAndGroupByFuncs(
				armadaslices.Concatenate(tc.ExpectedScheduledIndices, tc.ExpectedNeverAttemptedIndices),
				func(i int) int { return i },
				func(_ int) bool { return true },
			)
			var expectedUnsuccessfulIndices []int
			for i := range tc.Jobs {
				if _, ok := expectedSuccessfulOrNotAttemptedIndices[i]; !ok {
					expectedUnsuccessfulIndices = append(expectedUnsuccessfulIndices, i)
				}
			}
			expectedUnsuccessfulIndicesByQueue := armadaslices.GroupByFunc(
				expectedUnsuccessfulIndices,
				func(i int) string {
					return tc.Jobs[i].Queue()
				},
			)
			actualSuccessfulIndicesByQueue := make(map[string][]int)
			actualUnsuccessfulIndicesByQueue := make(map[string][]int)

			queues := armadaslices.Unique(append(
				maps.Keys(sctx.QueueSchedulingContexts),
				maps.Keys(queueNameToQueue)...,
			))
			for _, queue := range queues {
				qctx := sctx.QueueSchedulingContexts[queue]
				require.NotNil(t, queue)

				is := armadaslices.Map(
					maps.Keys(qctx.SuccessfulJobSchedulingContexts),
					func(jobId string) int {
						return indexByJobId[jobId]
					},
				)
				if len(is) > 0 {
					slices.Sort(is)
					actualSuccessfulIndicesByQueue[queue] = is
				}

				is = armadaslices.Map(
					maps.Keys(qctx.UnsuccessfulJobSchedulingContexts),
					func(jobId string) int {
						return indexByJobId[jobId]
					},
				)
				if len(is) > 0 {
					slices.Sort(is)
					actualUnsuccessfulIndicesByQueue[queue] = is
				}
			}
			assert.Equal(t, expectedScheduledIndicesByQueue, actualSuccessfulIndicesByQueue, "actual successful scheduling contexts does not match expected")
			assert.Equal(t, expectedUnsuccessfulIndicesByQueue, actualUnsuccessfulIndicesByQueue, "actual unsuccessful scheduling contexts does not match expected")

			// Check that job scheduling contexts contain a node if and only if successful.
			// This node must be the same as in the result.
			for _, qctx := range sctx.QueueSchedulingContexts {
				for _, jctx := range qctx.SuccessfulJobSchedulingContexts {
					assert.NotNil(t, jctx.PodSchedulingContext)
					assert.Equal(t, result.NodeIdByJobId[jctx.JobId], jctx.PodSchedulingContext.NodeId)
				}
				for _, jctx := range qctx.UnsuccessfulJobSchedulingContexts {
					if jctx.PodSchedulingContext != nil {
						assert.Equal(t, "", jctx.PodSchedulingContext.NodeId)
					}
				}
			}

			// Check that each scheduled job was allocated a node.
			for _, jctx := range result.ScheduledJobs {
				nodeId, ok := result.NodeIdByJobId[jctx.JobId]
				assert.True(t, ok)
				assert.NotEmpty(t, nodeId)

				node, err := nodeDb.GetNode(nodeId)
				require.NoError(t, err)
				assert.NotEmpty(t, node)

				// Check that the job can actually go onto this node.
				matches, reason, err := nodedb.StaticJobRequirementsMet(node, jctx)
				require.NoError(t, err)
				assert.Empty(t, reason)
				assert.True(t, matches)
			}

			// For jobs that could not be scheduled,
			// check that the number of excluded nodes equals the total number of nodes.
			for _, qctx := range sctx.QueueSchedulingContexts {
				for _, jctx := range qctx.UnsuccessfulJobSchedulingContexts {
					pctx := jctx.PodSchedulingContext
					if pctx == nil {
						// Skip jobs we never tried scheduling.
						continue
					}
					assert.Equal(t, nodeDb.NumNodes(), pctx.NumNodes)
					if gangId := jctx.GangInfo.Id; gangId == "" {
						numExcludedNodes := 0
						for _, count := range pctx.NumExcludedNodesByReason {
							numExcludedNodes += count
						}
						assert.Equal(t, nodeDb.NumNodes(), numExcludedNodes)
					}
				}
			}

			// Check that we were given a termination reason.
			assert.NotEmpty(t, sch.schedulingContext.TerminationReason)
		})
	}
}

func NewNodeDb(config configuration.SchedulingConfig, stringInterner *stringinterner.StringInterner) (*nodedb.NodeDb, error) {
	nodeDb, err := nodedb.NewNodeDb(
		config.PriorityClasses,
		config.IndexedResources,
		config.IndexedTaints,
		config.IndexedNodeLabels,
		config.WellKnownNodeTypes,
		testfixtures.TestResourceListFactory,
	)
	if err != nil {
		return nil, err
	}
	return nodeDb, nil
}

func TestQueueCandidateGangIteratorPQ_Ordering_BelowFairShare_EvenCurrentCost(t *testing.T) {
	queueA := &QueueCandidateGangIteratorItem{
		queue:             "A",
		proposedQueueCost: 2,
		currentQueueCost:  0,
		queueBudget:       5,
		itemSize:          2,
	}
	queueB := &QueueCandidateGangIteratorItem{
		queue:             "B",
		proposedQueueCost: 3,
		currentQueueCost:  0,
		queueBudget:       5,
		itemSize:          3,
	}
	queueC := &QueueCandidateGangIteratorItem{
		queue:             "C",
		proposedQueueCost: 1,
		currentQueueCost:  0,
		queueBudget:       5,
		itemSize:          1,
	}
	pq := &QueueCandidateGangIteratorPQ{prioritiseLargerJobs: true, items: []*QueueCandidateGangIteratorItem{queueA, queueB, queueC}}

	sort.Sort(pq)
	// Should be in order of biggest job as currentQueueCosts are all equal
	expectedOrder := []*QueueCandidateGangIteratorItem{queueB, queueA, queueC}
	assert.Equal(t, expectedOrder, pq.items)
}

func TestQueueCandidateGangIteratorPQ_Ordering_BelowFairShare_UnevenCurrentCost(t *testing.T) {
	queueA := &QueueCandidateGangIteratorItem{
		queue:             "A",
		proposedQueueCost: 4,
		currentQueueCost:  2,
		queueBudget:       5,
		itemSize:          2,
	}
	queueB := &QueueCandidateGangIteratorItem{
		queue:             "B",
		proposedQueueCost: 3,
		currentQueueCost:  2,
		queueBudget:       5,
		itemSize:          1,
	}
	queueC := &QueueCandidateGangIteratorItem{
		queue:             "C",
		proposedQueueCost: 2,
		currentQueueCost:  1,
		queueBudget:       5,
		itemSize:          1,
	}
	pq := &QueueCandidateGangIteratorPQ{prioritiseLargerJobs: true, items: []*QueueCandidateGangIteratorItem{queueA, queueB, queueC}}

	sort.Sort(pq)
	// Should be in order lowest current queue cost, then when current queue cost is equal it should be in order of largest job
	expectedOrder := []*QueueCandidateGangIteratorItem{queueC, queueA, queueB}
	assert.Equal(t, expectedOrder, pq.items)
}

func TestQueueCandidateGangIteratorPQ_Ordering_AboveFairShare(t *testing.T) {
	queueA := &QueueCandidateGangIteratorItem{
		queue:             "A",
		proposedQueueCost: 8,
		currentQueueCost:  6,
		queueBudget:       5,
		itemSize:          2,
	}
	queueB := &QueueCandidateGangIteratorItem{
		queue:             "B",
		proposedQueueCost: 7,
		currentQueueCost:  4,
		queueBudget:       5,
		itemSize:          3,
	}
	queueC := &QueueCandidateGangIteratorItem{
		queue:             "C",
		proposedQueueCost: 9,
		currentQueueCost:  8,
		queueBudget:       5,
		itemSize:          1,
	}
	pq := &QueueCandidateGangIteratorPQ{prioritiseLargerJobs: true, items: []*QueueCandidateGangIteratorItem{queueA, queueB, queueC}}

	sort.Sort(pq)
	// Should be in order of smallest amount over their fairshare
	expectedOrder := []*QueueCandidateGangIteratorItem{queueB, queueA, queueC}
	assert.Equal(t, expectedOrder, pq.items)
}

func TestQueueCandidateGangIteratorPQ_Ordering_MixedFairShare(t *testing.T) {
	aboveFairShare := &QueueCandidateGangIteratorItem{
		queue:             "A",
		proposedQueueCost: 8,
		currentQueueCost:  6,
		queueBudget:       5,
		itemSize:          2,
	}
	belowFairShare := &QueueCandidateGangIteratorItem{
		queue:             "B",
		proposedQueueCost: 3,
		currentQueueCost:  2,
		queueBudget:       5,
		itemSize:          1,
	}
	pq := &QueueCandidateGangIteratorPQ{prioritiseLargerJobs: true, items: []*QueueCandidateGangIteratorItem{aboveFairShare, belowFairShare}}

	sort.Sort(pq)
	expectedOrder := []*QueueCandidateGangIteratorItem{belowFairShare, aboveFairShare}
	assert.Equal(t, expectedOrder, pq.items)
}

func TestQueueCandidateGangIteratorPQ_Fallback(t *testing.T) {
	queueA := &QueueCandidateGangIteratorItem{
		queue: "A",
	}
	queueB := &QueueCandidateGangIteratorItem{
		queue: "B",
	}
	queueC := &QueueCandidateGangIteratorItem{
		queue: "C",
	}
	pq := &QueueCandidateGangIteratorPQ{prioritiseLargerJobs: true, items: []*QueueCandidateGangIteratorItem{queueB, queueC, queueA}}

	sort.Sort(pq)
	// Will fallback to ordering by queue name in the case all queues are the same sizes etc
	expectedOrder := []*QueueCandidateGangIteratorItem{queueA, queueB, queueC}
	assert.Equal(t, expectedOrder, pq.items)
}
