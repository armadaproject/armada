package scheduler

import (
	"context"
	"fmt"
	"testing"

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
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
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
		ExpectedNeverAttemptedIndices []int
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
		"MaximumJobsToSchedule": {
			SchedulingConfig: testfixtures.WithMaxJobsToScheduleConfig(2, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1),
				testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 10),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 3),
			),
			PriorityFactorByQueue:         map[string]float64{"A": 1},
			ExpectedScheduledIndices:      []int{0, 11},
			ExpectedNeverAttemptedIndices: []int{12, 13},
		},
		"MaximumGangsToSchedule": {
			SchedulingConfig: testfixtures.WithMaxGangsToScheduleConfig(2, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 2),
				),
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 2),
				),
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 2),
				),
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 2),
				),
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 2),
				),
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 2),
				),
			),
			PriorityFactorByQueue:         map[string]float64{"A": 1},
			ExpectedScheduledIndices:      []int{0, 1, 6, 7},
			ExpectedNeverAttemptedIndices: []int{8, 9, 10, 11},
		},
		"MaximumResourceFractionToSchedule": {
			SchedulingConfig: testfixtures.WithRoundLimitsConfig(
				map[string]float64{"cpu": 0.5},
				testfixtures.TestSchedulingConfig(),
			),
			PriorityFactorByQueue:         map[string]float64{"A": 1.0},
			Nodes:                         testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs:                          testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 32),
			ExpectedScheduledIndices:      testfixtures.IntRange(0, 16),
			ExpectedNeverAttemptedIndices: testfixtures.IntRange(17, 31),
		},
		"PerPriorityLimits": {
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
		"PerPriorityLimits equal MaximumResourceFractionToSchedule": {
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
		"MaxQueueLookback": {
			SchedulingConfig: testfixtures.WithMaxQueueLookbackConfig(3, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1),
				testfixtures.NLargeCpuJob("A", testfixtures.PriorityClass0, 3),
				testfixtures.NSmallCpuJob("A", testfixtures.PriorityClass0, 1),
			),
			PriorityFactorByQueue:         map[string]float64{"A": 1},
			ExpectedScheduledIndices:      []int{0},
			ExpectedNeverAttemptedIndices: []int{3, 4},
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
				"pool",
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
			assert.Equal(t, tc.ExpectedScheduledIndices, actualScheduledIndices, "actual scheduled indices does not match expected")

			// Check that the right job scheduling contexts were created.
			expectedScheduledIndicesByQueue := armadaslices.GroupByFunc(
				tc.ExpectedScheduledIndices,
				func(i int) string {
					return tc.Jobs[i].GetQueue()
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
					return tc.Jobs[i].GetQueue()
				},
			)
			actualSuccessfulIndicesByQueue := make(map[string][]int)
			actualUnsuccessfulIndicesByQueue := make(map[string][]int)

			queues := armadaslices.Unique(append(
				maps.Keys(sctx.QueueSchedulingContexts),
				maps.Keys(tc.PriorityFactorByQueue)...,
			))
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
			assert.Equal(t, expectedScheduledIndicesByQueue, actualSuccessfulIndicesByQueue, "actual successful scheduling contexts does not match expected")
			assert.Equal(t, expectedUnsuccessfulIndicesByQueue, actualUnsuccessfulIndicesByQueue, "actual unsuccessful scheduling contexts does not match expected")

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

func CreateNodeDb(nodes []*schedulerobjects.Node) (*nodedb.NodeDb, error) {
	db, err := nodedb.NewNodeDb(
		testfixtures.TestPriorityClasses,
		testfixtures.TestMaxExtraNodesToConsider,
		testfixtures.TestResources,
		testfixtures.TestIndexedTaints,
		testfixtures.TestIndexedNodeLabels,
	)
	if err != nil {
		return nil, err
	}
	if err := db.UpsertMany(nodes); err != nil {
		return nil, err
	}
	return db, nil
}
