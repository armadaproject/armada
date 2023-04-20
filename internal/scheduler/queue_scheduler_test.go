package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/util"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestQueueScheduler(t *testing.T) {
	tests := map[string]struct {
		SchedulingConfig configuration.SchedulingConfig
		// Minimum job size.
		MinimumJobSize map[string]resource.Quantity
		// Total resources across all clusters.
		// Set to the total resources across all nodes if not provided.
		TotalResources schedulerobjects.ResourceList
		// Map from queue to the priority factor associated with that queue.
		PriorityFactorByQueue map[string]float64
		// Initial resource usage for all queues.
		InitialAllocationByQueue map[string]schedulerobjects.QuantityByPriorityAndResourceType
		// Nodes to be considered by the scheduler.
		Nodes []*schedulerobjects.Node
		// Jobs to try scheduling.
		Jobs []*jobdb.Job
		// Indices of jobs expected to be scheduled.
		ExpectedScheduledIndices []int
		// Indices of jobs the scheduler should never have tried to schedule.
		// TODO: Rename to ExpectedNeverAttemptedIndices
		NeverAttemptedIndices []int
	}{
		"simple success": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			PriorityFactorByQueue:    map[string]float64{"A": 1.0},
			Nodes:                    testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:                     testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
			ExpectedScheduledIndices: testfixtures.IntRange(0, 31),
		},
		"simple failure": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			PriorityFactorByQueue:    map[string]float64{"A": 1.0},
			Nodes:                    testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:                     testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 33),
			ExpectedScheduledIndices: testfixtures.IntRange(0, 31),
		},
		"multiple nodes": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			PriorityFactorByQueue:    map[string]float64{"A": 1.0},
			Nodes:                    testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Jobs:                     testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 64),
			ExpectedScheduledIndices: testfixtures.IntRange(0, 63),
		},
		"preempt lower-priority jobs": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1), testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass1, 1)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: testfixtures.IntRange(0, 1),
		},
		"no preemption of higher-priority jobs": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass1, 1), testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: testfixtures.IntRange(0, 0),
		},
		"unschedulable jobs do not block schedulable jobs": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1), testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 10), testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{0, 11},
		},
		// TODO: Add max gangs scheduled checks.
		"max jobs to schedule limit": {
			SchedulingConfig:         testfixtures.WithMaxJobsToScheduleConfig(2, testfixtures.TestSchedulingConfig()),
			Nodes:                    testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:                     testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 5),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{0, 1},
			NeverAttemptedIndices:    []int{3, 4},
		},
		"round limits": {
			SchedulingConfig: testfixtures.WithRoundLimitsConfig(
				map[string]float64{"cpu": 2.0 / 32.0},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes:                    testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:                     testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 5),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{0, 1},
		},
		"round per-queue limits": {
			SchedulingConfig: testfixtures.WithPerQueueRoundLimitsConfig(map[string]float64{"cpu": 2.0 / 32.0}, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:             armadaslices.Concatenate(testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 5), testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 5)),
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
			ExpectedScheduledIndices: []int{0, 1, 5, 6},
		},
		"overall per-queue limits": {
			SchedulingConfig: testfixtures.WithPerQueueLimitsConfig(map[string]float64{"cpu": 2.0 / 32.0}, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:             armadaslices.Concatenate(testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 5), testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 5)),
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
			ExpectedScheduledIndices: []int{0, 1, 5, 6},
		},
		"overall per-queue limits with large memory amount": {
			SchedulingConfig: testfixtures.WithPerQueueLimitsConfig(
				map[string]float64{
					"cpu":    2.0 / 162975640.0,
					"memory": 0.1,
				},
				testfixtures.TestSchedulingConfig()),
			Nodes: testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:  armadaslices.Concatenate(testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 5), testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 5)),
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
			ExpectedScheduledIndices: []int{0, 1, 5, 6},
		},
		"overall per-queue limits with initial usage": {
			SchedulingConfig: testfixtures.WithPerQueueLimitsConfig(map[string]float64{"cpu": 2.0 / 32.0}, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:             armadaslices.Concatenate(testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 5), testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 5)),
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
			InitialAllocationByQueue: map[string]schedulerobjects.QuantityByPriorityAndResourceType{
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
			ExpectedScheduledIndices: []int{0, 1, 5},
		},
		"MaximalClusterFractionToSchedule": {
			SchedulingConfig: testfixtures.WithRoundLimitsConfig(
				map[string]float64{"cpu": 0.5},
				testfixtures.TestSchedulingConfig(),
			),
			PriorityFactorByQueue:    map[string]float64{"A": 1.0},
			Nodes:                    testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:                     testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
			ExpectedScheduledIndices: testfixtures.IntRange(0, 15),
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
			PriorityFactorByQueue: map[string]float64{"A": 1.0},
			Nodes:                 testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass3, 4),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass2, 8),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass1, 6),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 18),
			),
			ExpectedScheduledIndices: armadaslices.Concatenate(
				testfixtures.IntRange(0, 2),
				testfixtures.IntRange(4, 10),
				testfixtures.IntRange(12, 16),
				testfixtures.IntRange(18, 34),
			),
		},
		"per priority per queue limits equal limits": {
			SchedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[int32]map[string]float64{
					0: {"cpu": 0.9}, // 28 cpu
					1: {"cpu": 0.9},
				}, testfixtures.TestSchedulingConfig()),
			Nodes:                 testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:                  armadaslices.Concatenate(testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 5), testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 5)),
			PriorityFactorByQueue: map[string]float64{"A": 1},
			InitialAllocationByQueue: map[string]schedulerobjects.QuantityByPriorityAndResourceType{
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
			ExpectedScheduledIndices: []int{0},
		},
		"limit hit at higher priority doesn't block jobs at lower priority": {
			SchedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[int32]map[string]float64{
					0: {"cpu": 0.9}, // 28 cpu
					1: {"cpu": 0.5}, // 14 cpu
				}, testfixtures.TestSchedulingConfig()),
			Nodes:                 testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:                  armadaslices.Concatenate(testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass1, 1), testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 5)),
			PriorityFactorByQueue: map[string]float64{"A": 1},
			InitialAllocationByQueue: map[string]schedulerobjects.QuantityByPriorityAndResourceType{
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
			ExpectedScheduledIndices: []int{1},
		},
		"fairness two queues": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32), testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 32)),
			PriorityFactorByQueue:    map[string]float64{"A": 1, "B": 1},
			ExpectedScheduledIndices: armadaslices.Concatenate(testfixtures.IntRange(0, 15), testfixtures.IntRange(32, 47)),
		},
		"fairness three queues": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
				testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 32),
				testfixtures.NSmallCpuJob("C", testfixtures.PriorityClass0, 32),
			),
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
				"C": 1,
			},
			ExpectedScheduledIndices: armadaslices.Concatenate(
				testfixtures.IntRange(0, 10),
				testfixtures.IntRange(32, 42),
				testfixtures.IntRange(64, 73),
			),
		},
		"weighted fairness two queues": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(3, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 96),
				testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 96),
			),
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 2,
			},
			ExpectedScheduledIndices: armadaslices.Concatenate(
				testfixtures.IntRange(0, 63),
				testfixtures.IntRange(96, 127),
			),
		},
		"weighted fairness three queues": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(3, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 96),
				testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 96),
				testfixtures.NSmallCpuJob("C", testfixtures.PriorityClass0, 96),
			),
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 2,
				"C": 10,
			},
			ExpectedScheduledIndices: armadaslices.Concatenate(
				testfixtures.IntRange(0, 59),
				testfixtures.IntRange(96, 125),
				testfixtures.IntRange(192, 197),
			),
		},
		"fairness two queues with initial allocation": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
				testfixtures.NSmallCpuJob("B", testfixtures.PriorityClass0, 32),
			),
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
			InitialAllocationByQueue: map[string]schedulerobjects.QuantityByPriorityAndResourceType{
				"A": {
					0: schedulerobjects.ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("100"),
						},
					},
				},
			},
			ExpectedScheduledIndices: testfixtures.IntRange(32, 63),
		},
		"node with no available capacity": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: testfixtures.WithUsedResourcesNodes(
				0,
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			),
			Jobs:                     testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: nil,
		},
		"node with some available capacity": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: testfixtures.WithUsedResourcesNodes(
				0,
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("31"),
					},
				},
				testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			),
			Jobs:                     testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 2),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{0},
		},
		"preempt used resources of lower-priority jobs": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: testfixtures.WithUsedResourcesNodes(
				0,
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			),
			Jobs:                     testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass1, 1),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{0},
		},
		"respect taints": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.TestNTaintedCpuNode(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1), testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 1)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{1},
		},
		"minimum job size": {
			SchedulingConfig:      testfixtures.TestSchedulingConfig(),
			Nodes:                 testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:                  armadaslices.Concatenate(testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1), testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 1)),
			PriorityFactorByQueue: map[string]float64{"A": 1},
			MinimumJobSize: map[string]resource.Quantity{
				"cpu": resource.MustParse("2"),
			},
			ExpectedScheduledIndices: []int{1},
		},
		"minimum job size gpu": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNGpuNode(2, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1),
				testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 1),
				testfixtures.NGpuJob("A", testfixtures.PriorityClass0, 1),
			),
			PriorityFactorByQueue: map[string]float64{"A": 1},
			MinimumJobSize: map[string]resource.Quantity{
				"gpu": resource.MustParse("1"),
			},
			ExpectedScheduledIndices: []int{2},
		},
		"minimum job size two gpu": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNGpuNode(2, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1),
				testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 1),
				testfixtures.NGpuJob("A", testfixtures.PriorityClass0, 1),
			),
			PriorityFactorByQueue: map[string]float64{"A": 1},
			MinimumJobSize: map[string]resource.Quantity{
				"gpu": resource.MustParse("2"),
			},
			ExpectedScheduledIndices: nil,
		},
		"taints and tolerations": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.TestNTaintedCpuNode(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1), testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 1)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{1},
		},
		"Node selector": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: armadaslices.Concatenate(
				testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
				testfixtures.WithLabelsNodes(map[string]string{"foo": "foo"}, testfixtures.TestNCpuNode(1, testfixtures.TestPriorities)),
			),
			Jobs:                     testfixtures.WithNodeSelectorJobs(map[string]string{"foo": "foo"}, testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 2)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{0},
		},
		"taints and tolerations (indexed)": {
			SchedulingConfig:         testfixtures.WithIndexedTaintsConfig([]string{"largeJobsOnly"}, testfixtures.TestSchedulingConfig()),
			Nodes:                    testfixtures.TestNTaintedCpuNode(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1), testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 1)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{1},
		},
		"Node selector (indexed)": {
			SchedulingConfig: testfixtures.WithIndexedNodeLabelsConfig([]string{"foo"}, testfixtures.TestSchedulingConfig()),
			Nodes: append(
				testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
				testfixtures.WithLabelsNodes(map[string]string{"foo": "foo"}, testfixtures.TestNCpuNode(1, testfixtures.TestPriorities))...,
			),
			Jobs:                     testfixtures.WithNodeSelectorJobs(map[string]string{"foo": "foo"}, testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 2)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{0},
		},
		"QueueLeaseBatchSize": {
			SchedulingConfig: testfixtures.WithQueueLeaseBatchSizeConfig(3, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1),
				testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 3),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1),
			),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{0},
			NeverAttemptedIndices:    []int{3, 4},
		},
		"gang success": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Jobs:                     testfixtures.WithGangAnnotationsJobs(testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 2)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{0, 1},
		},
		"non-consecutive gang success": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(3, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.WithAnnotationsJobs(map[string]string{configuration.GangIdAnnotation: "my-gang", configuration.GangCardinalityAnnotation: "2"}, testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 1)),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1),
				testfixtures.WithAnnotationsJobs(map[string]string{configuration.GangIdAnnotation: "my-gang", configuration.GangCardinalityAnnotation: "2"}, testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 1)),
			),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{0, 1, 2},
		},
		"gang failure": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Jobs:                     testfixtures.WithGangAnnotationsJobs(testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 3)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: nil,
		},
		"non-consecutive gang failure": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.WithAnnotationsJobs(map[string]string{configuration.GangIdAnnotation: "my-gang", configuration.GangCardinalityAnnotation: "2"}, testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 1)),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1),
				testfixtures.WithAnnotationsJobs(map[string]string{configuration.GangIdAnnotation: "my-gang", configuration.GangCardinalityAnnotation: "2"}, testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 1)),
			),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{1},
		},
		"MaximalResourceFractionPerQueue non-consecutive gang": {
			SchedulingConfig: testfixtures.WithPerQueueLimitsConfig(
				map[string]float64{
					"cpu": 2.0 / 32.0,
				},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.WithAnnotationsJobs(map[string]string{configuration.GangIdAnnotation: "my-gang", configuration.GangCardinalityAnnotation: "2"}, testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1)),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1),
				testfixtures.WithAnnotationsJobs(map[string]string{configuration.GangIdAnnotation: "my-gang", configuration.GangCardinalityAnnotation: "2"}, testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1)),
			),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{1},
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
			indexByJobId := make(map[string]int)
			for i, job := range tc.Jobs {
				if _, ok := tc.PriorityFactorByQueue[job.GetQueue()]; !ok {
					panic(fmt.Sprintf("no priority factor for queue %s", job.Queue()))
				}
				indexByJobId[job.GetId()] = i
			}
			legacySchedulerJobs := make([]interfaces.LegacySchedulerJob, len(tc.Jobs))
			for i, job := range tc.Jobs {
				legacySchedulerJobs[i] = job
			}
			jobRepo := NewInMemoryJobRepository(tc.SchedulingConfig.Preemption.PriorityClasses)
			jobRepo.EnqueueMany(legacySchedulerJobs)

			sctx := schedulercontext.NewSchedulingContext(
				"executor",
				"pool",
				tc.SchedulingConfig.Preemption.PriorityClasses,
				tc.SchedulingConfig.Preemption.DefaultPriorityClass,
				tc.SchedulingConfig.ResourceScarcity,
				tc.PriorityFactorByQueue,
				tc.TotalResources,
				tc.InitialAllocationByQueue,
			)
			constraints := schedulerconstraints.SchedulingConstraintsFromSchedulingConfig(
				schedulerobjects.ResourceList{Resources: tc.MinimumJobSize},
				tc.SchedulingConfig,
			)
			jobIteratorByQueue := make(map[string]JobIterator)
			for queue := range tc.PriorityFactorByQueue {
				it, err := jobRepo.GetJobIterator(context.Background(), queue)
				require.NoError(t, err)
				jobIteratorByQueue[queue] = it
			}
			sch, err := NewQueueScheduler(sctx, constraints, nodeDb, jobIteratorByQueue)
			require.NoError(t, err)

			result, err := sch.Schedule(context.Background())
			require.NoError(t, err)

			// Check that the right jobs got scheduled.
			var actualScheduledIndices []int
			for _, job := range result.ScheduledJobs {
				actualScheduledIndices = append(
					actualScheduledIndices,
					indexByJobId[job.GetId()],
				)
			}
			slices.Sort(actualScheduledIndices)
			assert.Equal(t, tc.ExpectedScheduledIndices, actualScheduledIndices)

			// Check that the right job scheduling contexts were created.
			expectedScheduledIndicesByQueue := armadaslices.GroupByFunc(
				tc.ExpectedScheduledIndices,
				func(i int) string {
					return tc.Jobs[i].GetQueue()
				},
			)
			expectedSuccessfulOrNotAttemptedIndices := armadaslices.MapAndGroupByFuncs(
				armadaslices.Concatenate(tc.ExpectedScheduledIndices, tc.NeverAttemptedIndices),
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
					return tc.Jobs[i].GetQueue()
				},
			)
			actualSuccessfulIndicesByQueue := make(map[string][]int)
			actualUnsuccessfulIndicesByQueue := make(map[string][]int)

			queues := armadaslices.Unique(append(
				maps.Keys(sctx.QueueSchedulingContexts),
				maps.Keys(tc.PriorityFactorByQueue)...,
			),
			)
			for _, queue := range queues {
				qctx := sctx.QueueSchedulingContexts[queue]
				require.NotNil(t, queue)

				is := util.Map(
					maps.Keys(qctx.SuccessfulJobSchedulingContexts),
					func(jobId string) int {
						return indexByJobId[jobId]
					},
				)
				if len(is) > 0 {
					slices.Sort(is)
					actualSuccessfulIndicesByQueue[queue] = is
				}

				is = util.Map(
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
			assert.Equal(t, expectedScheduledIndicesByQueue, actualSuccessfulIndicesByQueue)
			assert.Equal(t, expectedUnsuccessfulIndicesByQueue, actualUnsuccessfulIndicesByQueue)

			// Check that job scheduling contexts contain a node if and only if successful.
			// This node must be the same as in the result.
			for _, qctx := range sctx.QueueSchedulingContexts {
				for _, jctx := range qctx.SuccessfulJobSchedulingContexts {
					assert.NotNil(t, jctx.PodSchedulingContext)
					assert.NotNil(t, jctx.PodSchedulingContext.Node)
					assert.Equal(t, result.NodeIdByJobId[jctx.JobId], jctx.PodSchedulingContext.Node.Id)
				}
				for _, jctx := range qctx.UnsuccessfulJobSchedulingContexts {
					if jctx.PodSchedulingContext != nil {
						assert.Nil(t, jctx.PodSchedulingContext.Node)
					}
				}
			}

			// Check that each scheduled job was allocated a node.
			for _, job := range result.ScheduledJobs {
				nodeId, ok := result.NodeIdByJobId[job.GetId()]
				assert.True(t, ok)
				assert.NotEmpty(t, nodeId)
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
					_, _, isGangJob, err := GangIdAndCardinalityFromLegacySchedulerJob(jctx.Job, nil)
					require.NoError(t, err)
					if !isGangJob {
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
		// Skip checking if contexts were generated.
		// Needed for tests where not all jobs are considered.
		DoNotCheckContexts bool
		// For each queue, the indices of jobs expected to be scheduled.
		ExpectedIndicesByQueue map[string][]int
		// For each queue, the expected resources assigned to jobs from that queue.
		ExpectedResourcesByQueue map[string]resourceLimits
		// ExpectedJobIndicesByNodeIndex[0]["A"] = [0, 2] indicates that
		// ReqsByQueue["A"][0] and ReqsByQueue["A"][0] expected to land on the 0-th node.
		ExpectedIndicesByNodeIndexAndQueue map[int]map[string][]int
	}{
		"one queue one job": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNSmallCpuJob("A", 0, 1),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"one queue several jobs": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNSmallCpuJob("A", 0, 32),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": testfixtures.IntRange(0, 31),
			},
			ExpectedIndicesByNodeIndexAndQueue: map[int]map[string][]int{
				0: {"A": testfixtures.IntRange(0, 31)},
			},
		},
		"one queue some jobs do not fit": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNSmallCpuJob("A", 0, 33),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": testfixtures.IntRange(0, 31),
			},
		},
		"one queue with jobs of varying size": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testfixtures.TestNSmallCpuJob("A", 0, 32), testfixtures.TestNLargeCpuJob("A", 0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": testfixtures.IntRange(0, 32),
			},
		},
		"preempt lower-priority jobs": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testfixtures.TestNSmallCpuJob("A", 0, 1), testfixtures.TestNLargeCpuJob("A", 1, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			// TODO: We could optimise this by un-leasing preempted jobs.
			ExpectedIndicesByQueue: map[string][]int{
				"A": testfixtures.IntRange(0, 1),
			},
		},
		"no preemption of higher-priority jobs": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testfixtures.TestNLargeCpuJob("A", 1, 1), testfixtures.TestNSmallCpuJob("A", 0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"unschedulable jobs do not block schedulable jobs": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(append(testfixtures.TestNSmallCpuJob("A", 0, 1), testfixtures.TestNLargeCpuJob("A", 0, 10)...), testfixtures.TestNSmallCpuJob("A", 0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 11},
			},
		},
		// TODO: Add max gangs scheduled checks.
		"max jobs to schedule limit": {
			SchedulingConfig: testfixtures.WithMaxJobsToScheduleConfig(2, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNSmallCpuJob("A", 0, 5),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			DoNotCheckContexts: true,
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 1},
			},
		},
		"round limits": {
			SchedulingConfig: testfixtures.WithRoundLimitsConfig(map[string]float64{"cpu": 2.0 / 32.0}, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNSmallCpuJob("A", 0, 5),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 1},
			},
		},
		"round per-queue limits": {
			SchedulingConfig: testfixtures.WithPerQueueRoundLimitsConfig(map[string]float64{"cpu": 2.0 / 32.0}, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNSmallCpuJob("A", 0, 5),
				"B": testfixtures.TestNSmallCpuJob("A", 0, 5),
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
			SchedulingConfig: testfixtures.WithPerQueueLimitsConfig(map[string]float64{"cpu": 2.0 / 32.0}, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNSmallCpuJob("A", 0, 5),
				"B": testfixtures.TestNSmallCpuJob("A", 0, 5),
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
			SchedulingConfig: testfixtures.WithPerQueueLimitsConfig(
				map[string]float64{
					"cpu":    2.0 / 162975640.0,
					"memory": 0.1,
				},
				testfixtures.TestSchedulingConfig()),
			Nodes: testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNSmallCpuJob("A", 0, 5),
				"B": testfixtures.TestNSmallCpuJob("A", 0, 5),
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
			SchedulingConfig: testfixtures.WithPerQueueLimitsConfig(map[string]float64{"cpu": 2.0 / 32.0}, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNSmallCpuJob("A", 0, 5),
				"B": testfixtures.TestNSmallCpuJob("A", 0, 5),
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
			SchedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[int32]map[string]float64{
					0: {"cpu": 1.0},
					1: {"cpu": 0.5},
					2: {"cpu": 0.25},
					3: {"cpu": 0.1},
				},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testfixtures.TestNSmallCpuJob("A", 3, 5), testfixtures.TestNSmallCpuJob("A", 0, 5)...),
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
			SchedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[int32]map[string]float64{
					0: {"cpu": 0.9}, // 28 cpu
					1: {"cpu": 0.9},
				}, testfixtures.TestSchedulingConfig()),
			Nodes: testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testfixtures.TestNSmallCpuJob("A", 0, 5), testfixtures.TestNSmallCpuJob("A", 0, 5)...),
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
			SchedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[int32]map[string]float64{
					0: {"cpu": 0.9}, // 28 cpu
					1: {"cpu": 0.5}, // 14 cpu
				}, testfixtures.TestSchedulingConfig()),
			Nodes: testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testfixtures.TestNSmallCpuJob("A", 1, 1), testfixtures.TestNSmallCpuJob("A", 0, 5)...),
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
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNSmallCpuJob("A", 0, 32),
				"B": testfixtures.TestNSmallCpuJob("A", 0, 32),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
			ExpectedResourcesByQueue: map[string]resourceLimits{
				"A": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("16")},
					map[string]resource.Quantity{"cpu": resource.MustParse("16")},
				),
				"B": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("16")},
					map[string]resource.Quantity{"cpu": resource.MustParse("16")},
				),
			},
		},
		"fairness three queues": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNSmallCpuJob("A", 0, 32),
				"B": testfixtures.TestNSmallCpuJob("A", 0, 32),
				"C": testfixtures.TestNSmallCpuJob("A", 0, 32),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
				"C": 1,
			},
			ExpectedResourcesByQueue: map[string]resourceLimits{
				"A": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("10")},
					map[string]resource.Quantity{"cpu": resource.MustParse("11")},
				),
				"B": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("10")},
					map[string]resource.Quantity{"cpu": resource.MustParse("11")},
				),
				"C": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("10")},
					map[string]resource.Quantity{"cpu": resource.MustParse("11")},
				),
			},
		},
		"weighted fairness two queues": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(3, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNSmallCpuJob("A", 0, 96),
				"B": testfixtures.TestNSmallCpuJob("A", 0, 96),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 2,
			},
			ExpectedResourcesByQueue: map[string]resourceLimits{
				"A": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("64")},
					map[string]resource.Quantity{"cpu": resource.MustParse("64")},
				),
				"B": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("32")},
					map[string]resource.Quantity{"cpu": resource.MustParse("32")},
				),
			},
		},
		"weighted fairness three queues": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(3, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNSmallCpuJob("A", 0, 96),
				"B": testfixtures.TestNSmallCpuJob("A", 0, 96),
				"C": testfixtures.TestNSmallCpuJob("A", 0, 96),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 2,
				"C": 10,
			},
			ExpectedResourcesByQueue: map[string]resourceLimits{
				"A": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("60")},
					map[string]resource.Quantity{"cpu": resource.MustParse("60")},
				),
				"B": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("30")},
					map[string]resource.Quantity{"cpu": resource.MustParse("30")},
				),
				"C": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("6")},
					map[string]resource.Quantity{"cpu": resource.MustParse("6")},
				),
			},
		},
		"fairness two queues with initial usage": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNSmallCpuJob("A", 0, 32),
				"B": testfixtures.TestNSmallCpuJob("A", 0, 32),
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
					map[string]resource.Quantity{"cpu": resource.MustParse("0")},
				),
				"B": newResourceLimits(
					map[string]resource.Quantity{"cpu": resource.MustParse("32")},
					map[string]resource.Quantity{"cpu": resource.MustParse("32")},
				),
			},
		},
		"node with no available capacity": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: testfixtures.WithUsedResourcesNodes(
				0,
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNSmallCpuJob("A", 0, 1),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": nil,
			},
		},
		"node with some available capacity": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: testfixtures.WithUsedResourcesNodes(
				0,
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("31"),
					},
				},
				testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNSmallCpuJob("A", 0, 2),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"preempt used resources of lower-priority jobs": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: testfixtures.WithUsedResourcesNodes(
				0,
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.TestNLargeCpuJob("A", 1, 1),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"respect taints": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNTaintedCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testfixtures.TestNSmallCpuJob("A", 0, 1), testfixtures.TestNLargeCpuJob("A", 0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {1},
			},
		},
		"minimum job size": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testfixtures.TestNSmallCpuJob("A", 0, 1), testfixtures.TestNLargeCpuJob("A", 0, 1)...),
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
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNGpuNode(2, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(append(testfixtures.TestNSmallCpuJob("A", 0, 1), testfixtures.TestNLargeCpuJob("A", 0, 1)...), testfixtures.TestNGpuJob("A", 0, 1)...),
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
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNGpuNode(2, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(append(testfixtures.TestNSmallCpuJob("A", 0, 1), testfixtures.TestNLargeCpuJob("A", 0, 1)...), testfixtures.TestNGpuJob("A", 0, 1)...),
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
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNTaintedCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testfixtures.TestNSmallCpuJob("A", 0, 1), testfixtures.TestNLargeCpuJob("A", 0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {1},
			},
		},
		"Node selector": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: append(
				testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
				testfixtures.WithLabelsNodes(map[string]string{"foo": "foo"}, testfixtures.TestNCpuNode(1, testfixtures.TestPriorities))...,
			),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.WithNodeSelectorPodReqs(map[string]string{"foo": "foo"}, testfixtures.TestNLargeCpuJob("A", 0, 2)),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"taints and tolerations (indexed)": {
			SchedulingConfig: testfixtures.WithIndexedTaintsConfig([]string{"largeJobsOnly"}, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.TestNTaintedCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testfixtures.TestNSmallCpuJob("A", 0, 1), testfixtures.TestNLargeCpuJob("A", 0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {1},
			},
		},
		"Node selector (indexed)": {
			SchedulingConfig: testfixtures.WithIndexedNodeLabelsConfig([]string{"foo"}, testfixtures.TestSchedulingConfig()),
			Nodes: append(
				testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
				testfixtures.WithLabelsNodes(map[string]string{"foo": "foo"}, testfixtures.TestNCpuNode(1, testfixtures.TestPriorities))...,
			),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.WithNodeSelectorPodReqs(map[string]string{"foo": "foo"}, testfixtures.TestNLargeCpuJob("A", 0, 2)),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"QueueLeaseBatchSize Respected": {
			SchedulingConfig: testfixtures.WithQueueLeaseBatchSizeConfig(3, testfixtures.TestSchedulingConfig()), // should quit after 3 unschedulable jobs
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),                          // 32 cores
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(append(testfixtures.TestNSmallCpuJob("A", 0, 1), testfixtures.TestNLargeCpuJob("A", 0, 3)...), testfixtures.TestNSmallCpuJob("A", 0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"gang scheduling success": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.WithGangAnnotationsPodReqs(testfixtures.TestNLargeCpuJob("A", 0, 2)),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 1},
			},
		},
		"gang scheduling failure": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testfixtures.WithGangAnnotationsPodReqs(testfixtures.TestNLargeCpuJob("A", 0, 3)),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {},
			},
		},
		"gang aggregated resource accounting": {
			SchedulingConfig: testfixtures.WithPerQueueLimitsConfig(
				map[string]float64{
					"cpu": 2.0 / 32.0,
				},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(append(
					testfixtures.WithAnnotationsPodReqs(map[string]string{configuration.GangIdAnnotation: "my-gang", configuration.GangCardinalityAnnotation: "2"}, testfixtures.TestNSmallCpuJob("A", 0, 1)),
					testfixtures.TestNSmallCpuJob("A", 0, 1)...),
					testfixtures.WithAnnotationsPodReqs(map[string]string{configuration.GangIdAnnotation: "my-gang", configuration.GangCardinalityAnnotation: "2"}, testfixtures.TestNSmallCpuJob("A", 0, 1))...,
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
			// TODO: Use InMemoryRepo instead.
			indexByJobId := make(map[string]int)
			jobRepository := newMockJobRepository()
			for queue, reqs := range tc.ReqsByQueue {
				jobs := apiJobsFromPodReqs(queue, reqs)
				jobRepository.EnqueueMany(jobs)
				for i, job := range jobs {
					indexByJobId[job.GetId()] = i
				}
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

			nodeDb, err := CreateNodeDb(tc.Nodes)
			if !assert.NoError(t, err) {
				return
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
				tc.InitialUsageByQueue,
			)
			constraints := schedulerconstraints.SchedulingConstraintsFromSchedulingConfig(
				schedulerobjects.ResourceList{Resources: tc.MinimumJobSize},
				tc.SchedulingConfig,
			)
			jobIteratorByQueue := make(map[string]JobIterator)
			for queue := range tc.PriorityFactorByQueue {
				it, err := jobRepository.GetJobIterator(context.Background(), name)
				require.NoError(t, err)
				jobIteratorByQueue[queue] = it
			}
			sch, err := NewQueueScheduler(
				sctx,
				constraints,
				nodeDb,
				jobIteratorByQueue,
			)
			require.NoError(t, err)
			result, err := sch.Schedule(context.Background())
			require.NoError(t, err)
			require.Empty(t, result.PreemptedJobs)

			// Check that the right jobs got scheduled.
			if tc.ExpectedIndicesByQueue != nil {
				actualScheduledIndicesByQueue := make(map[string][]int)
				for _, job := range result.ScheduledJobs {
					actualScheduledIndicesByQueue[job.GetQueue()] = append(
						actualScheduledIndicesByQueue[job.GetQueue()],
						indexByJobId[job.GetId()],
					)
				}
				assert.Equal(t, tc.ExpectedIndicesByQueue, actualScheduledIndicesByQueue)
				// actualScheduledJobsByQueue := jobIdsByQueueFromJobs(result.ScheduledJobs)
				// for queue, expected := range expectedByQueue {
				// 	actual, ok := actualScheduledJobsByQueue[queue]
				// 	if !ok {
				// 		// Since expected is []string{} if we expect no jobs to be scheduled.
				// 		actual = make([]string, 0)
				// 	}
				// 	assert.Equal(t, expected, actual, "queue %s", queue)
				// }
			}

			// Check that each queue was allocated the right amount of resources.
			if tc.ExpectedResourcesByQueue != nil {
				actualUsageByQueue := usageByQueue(result.ScheduledJobs, tc.SchedulingConfig.Preemption.PriorityClasses)
				for queue, usage := range actualUsageByQueue {
					assertResourceLimitsSatisfied(t, tc.ExpectedResourcesByQueue[queue], usage)
				}
			}

			// Check that each job is allocated to a node.
			for _, job := range result.ScheduledJobs {
				nodeId, ok := result.NodeIdByJobId[job.GetId()]
				assert.True(t, ok)
				assert.NotEmpty(t, nodeId)
			}

			// Check that scheduling contexts were generated.
			// TODO: Check that contexts correctly indicate success/not.
			// TODO: Always check contexts?
			if !tc.DoNotCheckContexts {
				// Check that started and finished times are set.
				assert.NotEqual(t, time.Time{}, sch.schedulingContext.Started)
				assert.NotEqual(t, time.Time{}, sch.schedulingContext.Finished)

				// Check that scheduled resources is set correctly.
				for queue, expected := range usageByQueueAndPriority(result.ScheduledJobs, tc.SchedulingConfig.Preemption.PriorityClasses) {
					qctx, ok := sch.schedulingContext.QueueSchedulingContexts[queue]
					require.NotNil(t, qctx)
					require.True(t, ok)

					// Scheduling round accounting.
					actual := qctx.ScheduledResourcesByPriority
					assert.True(t, expected.Equal(actual), "expected %v, but got %v", expected, actual)

					// TODO: Test usage.
					// // Scheduler result accounting.
					// actual = result.UsageByQueueAndPriority[queue]
					// assert.True(t, expected.Equal(actual), "expected %v, but got %v", expected, actual)
				}

				// // Check that the scheduling context includes all queues and jobs.
				// assert.Equal(
				// 	t,
				// 	len(tc.PriorityFactorByQueue),
				// 	len(sched.SchedulingContext.QueueSchedulingContexts),
				// )
				leasedJobIds := make(map[string]interface{})
				for _, job := range result.ScheduledJobs {
					leasedJobIds[job.GetId()] = true
				}
				for queue, jobs := range jobRepository.jobsByQueue {
					qctx, ok := sch.schedulingContext.QueueSchedulingContexts[queue]
					if !assert.NotNil(t, qctx) {
						continue
					}
					if !assert.True(t, ok) {
						continue
					}

					for i, job := range jobs {
						if i >= int(tc.SchedulingConfig.QueueLeaseBatchSize) {
							break
						}

						_, isLeased := leasedJobIds[job.GetId()]
						var jctxByJobId map[string]*schedulercontext.JobSchedulingContext
						if isLeased {
							jctxByJobId = qctx.SuccessfulJobSchedulingContexts
						} else {
							jctxByJobId = qctx.UnsuccessfulJobSchedulingContexts
						}
						if !assert.NotNil(t, jctxByJobId) {
							continue
						}

						jctx, ok := jctxByJobId[job.GetId()]
						if !assert.True(t, ok, "missing context for job; leased: %v", isLeased) {
							continue
						}
						if !assert.NotNil(t, jctx) {
							continue
						}
					}

					// For jobs that could not be scheduled,
					// check that the number of excluded nodes equals the total number of nodes.
					for _, qctx := range sch.schedulingContext.QueueSchedulingContexts {
						for _, jctx := range qctx.UnsuccessfulJobSchedulingContexts {
							pctx := jctx.PodSchedulingContext
							if pctx == nil {
								// Skip jobs we never tried scheduling.
								continue
							}
							// TODO: Remove?
							// if pctx.Node != nil {
							// 	// To avoid checking pod scheduling contexts for gang jobs that succeeded
							// 	// (we include these in failed job scheduling contexts).
							// 	continue
							// }
							assert.Equal(t, nodeDb.NumNodes(), pctx.NumNodes)
							numExcludedNodes := 0
							for _, count := range pctx.NumExcludedNodesByReason {
								numExcludedNodes += count
							}
							assert.Equal(t, nodeDb.NumNodes(), numExcludedNodes)
						}
					}
				}

				// Check that total resources is correct.
				// TODO: This should be tested in the context package.
				assert.True(t, sch.schedulingContext.TotalResources.Equal(tc.TotalResources))

				// Check that we were given a termination reason.
				// TODO: This should be tested in the context package.
				assert.NotEmpty(t, sch.schedulingContext.TerminationReason)
			}
		})
	}
}
