package scheduling

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
	testclock "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadaconfiguration "github.com/armadaproject/armada/internal/common/constants"
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
				testfixtures.WithGangJobDetails(testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1), "my-gang", 2, ""),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.WithGangJobDetails(testfixtures.N32Cpu256GiJobsWithLargeJobToleration("A", testfixtures.PriorityClass0, 1), "my-gang", 2, ""),
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
			jobRepo := NewInMemoryJobRepository(testfixtures.TestPool, jobdb.JobPriorityComparer{})
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
			sch, err := NewQueueScheduler(sctx, constraints, testfixtures.TestEmptyFloatingResources, nodeDb, jobIteratorByQueue, false, false, tc.SchedulingConfig.EnablePreferLargeJobOrdering, tc.SchedulingConfig.MaxQueueLookback, false, 0, clock.RealClock{}, 0)
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
				}
				for _, jctx := range qctx.UnsuccessfulJobSchedulingContexts {
					if jctx.PodSchedulingContext != nil {
						assert.Equal(t, "", jctx.PodSchedulingContext.NodeId)
					}
				}
			}

			// Check that each scheduled job was allocated a node.
			for _, jctx := range result.ScheduledJobs {
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
					if !jctx.Job.IsInGang() {
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

// steppingClock advances time by a fixed step on each Now() call.
// This is used to test soft timeout: since the scheduler calls Now() during iteration,
// the stepping clock ensures elapsed time exceeds the soft timeout threshold.
type steppingClock struct {
	fakeClock *testclock.FakeClock
	step      time.Duration
}

func newSteppingClock(start time.Time, step time.Duration) *steppingClock {
	return &steppingClock{
		fakeClock: testclock.NewFakeClock(start),
		step:      step,
	}
}

func (c *steppingClock) Now() time.Time {
	now := c.fakeClock.Now()
	c.fakeClock.Step(c.step)
	return now
}

func (c *steppingClock) Since(t time.Time) time.Duration {
	return c.fakeClock.Now().Sub(t)
}

func (c *steppingClock) NewTimer(d time.Duration) clock.Timer {
	return c.fakeClock.NewTimer(d)
}

func (c *steppingClock) NewTicker(d time.Duration) clock.Ticker {
	return c.fakeClock.NewTicker(d)
}

func (c *steppingClock) Sleep(d time.Duration) {
	c.fakeClock.Sleep(d)
}

func (c *steppingClock) After(d time.Duration) <-chan time.Time {
	return c.fakeClock.After(d)
}

func (c *steppingClock) Tick(d time.Duration) <-chan time.Time {
	return c.fakeClock.Tick(d)
}

type timeoutTestSetup struct {
	config      configuration.SchedulingConfig
	nodeDb      *nodedb.NodeDb
	sctx        *context.SchedulingContext
	constraints schedulerconstraints.SchedulingConstraints
}

func setupTimeoutTest(t *testing.T, queues []*api.Queue, jobsByQueue map[string][]*jobdb.Job, numNodes int) *timeoutTestSetup {
	t.Helper()
	config := testfixtures.TestSchedulingConfig()

	nodeDb, err := NewNodeDb(config, stringinterner.New(1024))
	require.NoError(t, err)

	txn := nodeDb.Txn(true)
	for _, node := range testfixtures.N32CpuNodes(numNodes, testfixtures.TestPriorities) {
		require.NoError(t, nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node.DeepCopyNilKeys()))
	}
	txn.Commit()
	totalResources := nodeDb.TotalKubernetesResources()

	fairnessCostProvider, err := fairness.NewDominantResourceFairness(totalResources, testfixtures.TestPool, config)
	require.NoError(t, err)

	sctx := context.NewSchedulingContext(
		testfixtures.TestPool,
		fairnessCostProvider,
		rate.NewLimiter(rate.Limit(config.MaximumSchedulingRate), config.MaximumSchedulingBurst),
		totalResources,
	)

	for _, queue := range queues {
		jobs := jobsByQueue[queue.Name]
		demand := testfixtures.TestResourceListFactory.MakeAllZero()
		for _, job := range jobs {
			demand = demand.Add(job.AllResourceRequirements())
		}
		err = sctx.AddQueueSchedulingContext(
			queue.Name, queue.PriorityFactor, queue.PriorityFactor,
			nil, demand, demand, internaltypes.ResourceList{},
			rate.NewLimiter(rate.Limit(config.MaximumPerQueueSchedulingRate), config.MaximumPerQueueSchedulingBurst),
		)
		require.NoError(t, err)
	}

	constraints := schedulerconstraints.NewSchedulingConstraints(testfixtures.TestPool, totalResources, config, queues)

	return &timeoutTestSetup{
		config:      config,
		nodeDb:      nodeDb,
		sctx:        sctx,
		constraints: constraints,
	}
}

func markJobsAsEvicted(jctxs []*context.JobSchedulingContext) {
	for _, jctx := range jctxs {
		jctx.IsEvicted = true
	}
}

func (s *timeoutTestSetup) createScheduler(clk clock.Clock, softTimeout time.Duration, jobIteratorsByQueue map[string]JobContextIterator) (*QueueScheduler, error) {
	return NewQueueScheduler(
		s.sctx, s.constraints, nil, s.nodeDb, jobIteratorsByQueue,
		false, false, true, s.config.MaxQueueLookback, false, 0,
		clk, softTimeout,
	)
}

func createJobRepoWithJobs(jctxs ...[]*context.JobSchedulingContext) *InMemoryJobRepository {
	jobRepo := NewInMemoryJobRepository(testfixtures.TestPool, jobdb.JobPriorityComparer{})
	for _, batch := range jctxs {
		jobRepo.EnqueueMany(batch)
	}
	return jobRepo
}

func TestQueuedGangIterator(t *testing.T) {
	tests := map[string]struct {
		jctxs                 []*context.JobSchedulingContext
		maxLookback           uint
		onlyYieldEvicted      bool
		expectReturnedIndexes []int
	}{
		"iterates all jobs": {
			jctxs: []*context.JobSchedulingContext{
				createJctx(false),
				createJctx(false),
				createJctx(true),
			},
			maxLookback:           1000,
			expectReturnedIndexes: []int{0, 1, 2},
		},
		"max lookback": {
			jctxs: []*context.JobSchedulingContext{
				createJctx(false),
				createJctx(false),
				createJctx(false),
				createJctx(false),
			},
			maxLookback:           2,
			expectReturnedIndexes: []int{0, 1},
		},
		"max lookback - still returns all evicted jobs": {
			jctxs: []*context.JobSchedulingContext{
				createJctx(false),
				createJctx(false),
				createJctx(true),
				createJctx(true),
			},
			maxLookback:           1,
			expectReturnedIndexes: []int{0, 2, 3},
		},
		"only yield evicted": {
			jctxs: []*context.JobSchedulingContext{
				createJctx(false),
				createJctx(true),
				createJctx(true),
				createJctx(false),
			},
			maxLookback:           1000,
			onlyYieldEvicted:      true,
			expectReturnedIndexes: []int{1, 2},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			sctx := context.NewSchedulingContext("pool", nil, nil, testfixtures.TestResourceListFactory.MakeAllZero())
			it := NewQueuedGangIterator(sctx, NewInMemoryJobIterator(tc.jctxs), tc.maxLookback, false)
			if tc.onlyYieldEvicted {
				it.OnlyYieldEvicted()
			}

			expected := []string{}
			for _, index := range tc.expectReturnedIndexes {
				expected = append(expected, tc.jctxs[index].JobId)
			}

			result := make([]string, 0)
			for {
				gctx, err := it.Peek()
				require.NoError(t, err)
				if gctx == nil {
					break
				}
				assert.Len(t, gctx.JobSchedulingContexts, 1)
				result = append(result, gctx.JobSchedulingContexts[0].Job.Id())
				it.Clear()
			}

			assert.Equal(t, expected, result)
		})
	}
}

func TestQueuedGangIterator_OnlyYieldEvicted_TopItemEvicted(t *testing.T) {
	jctxs := []*context.JobSchedulingContext{
		createJctx(true),
		createJctx(true),
		createJctx(true),
	}

	sctx := context.NewSchedulingContext("pool", nil, nil, testfixtures.TestResourceListFactory.MakeAllZero())
	it := NewQueuedGangIterator(sctx, NewInMemoryJobIterator(jctxs), 1, false)

	gctx, err := it.Peek()
	require.NoError(t, err)
	assert.Len(t, gctx.JobSchedulingContexts, 1)
	assert.Equal(t, jctxs[0].JobId, gctx.JobSchedulingContexts[0].JobId)

	it.OnlyYieldEvicted()

	// No change expected as first job is evicted
	gctx, err = it.Peek()
	require.NoError(t, err)
	assert.Len(t, gctx.JobSchedulingContexts, 1)
	assert.Equal(t, jctxs[0].JobId, gctx.JobSchedulingContexts[0].JobId)
}

func TestQueuedGangIterator_OnlyYieldEvicted_TopItemNonEvicted(t *testing.T) {
	jctxs := []*context.JobSchedulingContext{
		createJctx(false),
		createJctx(true),
		createJctx(true),
	}

	sctx := context.NewSchedulingContext("pool", nil, nil, testfixtures.TestResourceListFactory.MakeAllZero())
	it := NewQueuedGangIterator(sctx, NewInMemoryJobIterator(jctxs), 1, false)

	gctx, err := it.Peek()
	require.NoError(t, err)
	assert.Len(t, gctx.JobSchedulingContexts, 1)
	assert.Equal(t, jctxs[0].JobId, gctx.JobSchedulingContexts[0].JobId)

	it.OnlyYieldEvicted()

	gctx, err = it.Peek()
	require.NoError(t, err)
	assert.Len(t, gctx.JobSchedulingContexts, 1)
	assert.Equal(t, jctxs[1].JobId, gctx.JobSchedulingContexts[0].JobId)
}

func createJctx(evicted bool) *context.JobSchedulingContext {
	job := testfixtures.Test1Cpu4GiJob("A", testfixtures.PriorityClass0)
	jctx := context.JobSchedulingContextFromJob(job)
	jctx.IsEvicted = evicted
	return jctx
}

func TestQueueSchedulerTimeouts(t *testing.T) {
	tests := map[string]struct {
		softTimeout       time.Duration
		cancelContext     bool
		expectError       bool
		expectedScheduled int
	}{
		"soft timeout: schedules only evicted jobs": {
			softTimeout:       1 * time.Nanosecond,
			expectedScheduled: 50,
		},
		"hard timeout: returns error": {
			cancelContext: true,
			expectError:   true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			queue := &api.Queue{Name: "A", PriorityFactor: 1.0}
			// 50 evicted + 100 new jobs; with soft timeout only the 50 evicted should be scheduled
			evictedJobs := testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 50)
			newJobs := testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 100)

			evictedJctxs := context.JobSchedulingContextsFromJobs(evictedJobs)
			markJobsAsEvicted(evictedJctxs)

			allJobs := armadaslices.Concatenate(evictedJobs, newJobs)
			// 10 nodes * 32 CPUs = 320 CPUs total, enough capacity for all 150 jobs
			setup := setupTimeoutTest(t, []*api.Queue{queue}, map[string][]*jobdb.Job{"A": allJobs}, 10)
			jobIteratorsByQueue := map[string]JobContextIterator{
				"A": createJobRepoWithJobs(evictedJctxs, context.JobSchedulingContextsFromJobs(newJobs)).GetJobIterator("A"),
			}

			var clk clock.Clock = clock.RealClock{}
			if tc.softTimeout > 0 {
				// Step 10s per Now() call ensures soft timeout (1ns) is exceeded immediately
				clk = newSteppingClock(time.Now(), 10*time.Second)
			}
			sch, err := setup.createScheduler(clk, tc.softTimeout, jobIteratorsByQueue)
			require.NoError(t, err)

			ctx := armadacontext.Background()
			if tc.cancelContext {
				var cancel func()
				ctx, cancel = armadacontext.WithCancel(ctx)
				cancel()
			}

			result, err := sch.Schedule(ctx)

			if tc.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Len(t, result.ScheduledJobs, tc.expectedScheduled)
				// Verify all scheduled jobs are evicted (soft timeout only allows evicted jobs)
				if tc.softTimeout > 0 {
					for _, jctx := range result.ScheduledJobs {
						assert.True(t, jctx.IsEvicted, "only evicted jobs should be scheduled after soft timeout")
					}
				}
			}
		})
	}
}
