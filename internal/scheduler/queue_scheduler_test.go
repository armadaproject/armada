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
	"github.com/armadaproject/armada/internal/scheduler/fairness"
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
		InitialAllocatedByQueueAndPriorityClass map[string]schedulerobjects.QuantityByTAndResourceType[string]
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
			Nodes:                    testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
			ExpectedScheduledIndices: testfixtures.IntRange(0, 31),
		},
		"simple failure": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			PriorityFactorByQueue:    map[string]float64{"A": 1.0},
			Nodes:                    testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 33),
			ExpectedScheduledIndices: testfixtures.IntRange(0, 31),
		},
		"multiple nodes": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			PriorityFactorByQueue:    map[string]float64{"A": 1.0},
			Nodes:                    testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Jobs:                     testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 64),
			ExpectedScheduledIndices: testfixtures.IntRange(0, 63),
		},
		"preempt lower-priority jobs": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1), testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 1)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: testfixtures.IntRange(0, 1),
		},
		"no preemption of higher-priority jobs": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass1, 1), testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: testfixtures.IntRange(0, 0),
		},
		"unschedulable jobs do not block schedulable jobs": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1), testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 10), testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{0, 11},
		},
		"MaximumJobsToSchedule": {
			SchedulingConfig: testfixtures.WithMaxJobsToScheduleConfig(2, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 10),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 3),
			),
			PriorityFactorByQueue:         map[string]float64{"A": 1},
			ExpectedScheduledIndices:      []int{0, 11},
			ExpectedNeverAttemptedIndices: []int{12, 13},
		},
		"MaximumGangsToSchedule": {
			SchedulingConfig: testfixtures.WithMaxGangsToScheduleConfig(2, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 2),
				),
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 2),
				),
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 2),
				),
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 2),
				),
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 2),
				),
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 2),
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
			PriorityFactorByQueue: map[string]float64{"A": 1.0},
			Nodes:                 testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
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
		"fairness two queues": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32), testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 32)),
			PriorityFactorByQueue:    map[string]float64{"A": 1, "B": 1},
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
			Nodes:            testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 96),
				testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 96),
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
			Nodes:            testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 96),
				testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 96),
				testfixtures.N1Cpu4GiJobs("C", testfixtures.PriorityClass0, 96),
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
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
				testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 32),
			),
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
			InitialAllocatedByQueueAndPriorityClass: map[string]schedulerobjects.QuantityByTAndResourceType[string]{
				"A": {
					testfixtures.PriorityClass0: schedulerobjects.ResourceList{
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
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			),
			Jobs:                     testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
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
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			),
			Jobs:                     testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 2),
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
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			),
			Jobs:                     testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass1, 1),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{0},
		},
		"respect taints": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.NTainted32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1), testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{1},
		},
		"minimum job size": {
			SchedulingConfig:      testfixtures.TestSchedulingConfig(),
			Nodes:                 testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                  armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1), testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
			PriorityFactorByQueue: map[string]float64{"A": 1},
			MinimumJobSize: map[string]resource.Quantity{
				"cpu": resource.MustParse("2"),
			},
			ExpectedScheduledIndices: []int{1},
		},
		"minimum job size gpu": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N8GpuNodes(2, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N1GpuJobs("A", testfixtures.PriorityClass0, 1),
			),
			PriorityFactorByQueue: map[string]float64{"A": 1},
			MinimumJobSize: map[string]resource.Quantity{
				"gpu": resource.MustParse("1"),
			},
			ExpectedScheduledIndices: []int{2},
		},
		"minimum job size two gpu": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N8GpuNodes(2, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N1GpuJobs("A", testfixtures.PriorityClass0, 1),
			),
			PriorityFactorByQueue: map[string]float64{"A": 1},
			MinimumJobSize: map[string]resource.Quantity{
				"gpu": resource.MustParse("2"),
			},
			ExpectedScheduledIndices: nil,
		},
		"taints and tolerations": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.NTainted32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1), testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{1},
		},
		"Node selector": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: armadaslices.Concatenate(
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				testfixtures.WithLabelsNodes(map[string]string{"foo": "foo"}, testfixtures.N32CpuNodes(1, testfixtures.TestPriorities)),
			),
			Jobs:                     testfixtures.WithNodeSelectorJobs(map[string]string{"foo": "foo"}, testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 2)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{0},
		},
		"taints and tolerations (indexed)": {
			SchedulingConfig:         testfixtures.WithIndexedTaintsConfig([]string{"largeJobsOnly"}, testfixtures.TestSchedulingConfig()),
			Nodes:                    testfixtures.NTainted32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1), testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{1},
		},
		"Node selector (indexed)": {
			SchedulingConfig: testfixtures.WithIndexedNodeLabelsConfig([]string{"foo"}, testfixtures.TestSchedulingConfig()),
			Nodes: append(
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				testfixtures.WithLabelsNodes(map[string]string{"foo": "foo"}, testfixtures.N32CpuNodes(1, testfixtures.TestPriorities))...,
			),
			Jobs:                     testfixtures.WithNodeSelectorJobs(map[string]string{"foo": "foo"}, testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 2)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{0},
		},
		"MaxQueueLookback": {
			SchedulingConfig: testfixtures.WithMaxQueueLookbackConfig(3, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 3),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
			),
			PriorityFactorByQueue:         map[string]float64{"A": 1},
			ExpectedScheduledIndices:      []int{0},
			ExpectedNeverAttemptedIndices: []int{3, 4},
		},
		"gang success": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Jobs:                     testfixtures.WithGangAnnotationsJobs(testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 2)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{0, 1},
		},
		"non-consecutive gang success": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.WithAnnotationsJobs(map[string]string{configuration.GangIdAnnotation: "my-gang", configuration.GangCardinalityAnnotation: "2"}, testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.WithAnnotationsJobs(map[string]string{configuration.GangIdAnnotation: "my-gang", configuration.GangCardinalityAnnotation: "2"}, testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
			),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{0, 1, 2},
		},
		"gang failure": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Jobs:                     testfixtures.WithGangAnnotationsJobs(testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 3)),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: nil,
		},
		"non-consecutive gang failure": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.WithAnnotationsJobs(map[string]string{configuration.GangIdAnnotation: "my-gang", configuration.GangCardinalityAnnotation: "2"}, testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.WithAnnotationsJobs(map[string]string{configuration.GangIdAnnotation: "my-gang", configuration.GangCardinalityAnnotation: "2"}, testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
			),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{1},
		},
		"job priority": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.WithPriorityJobs(10, testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithPriorityJobs(1, testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithPriorityJobs(20, testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
			),
			PriorityFactorByQueue:    map[string]float64{"A": 1},
			ExpectedScheduledIndices: []int{1},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := NewNodeDb()
			require.NoError(t, err)
			txn := nodeDb.Txn(true)
			for _, node := range tc.Nodes {
				err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node)
				require.NoError(t, err)
			}
			txn.Commit()
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

			fairnessCostProvider, err := fairness.NewDominantResourceFairness(
				tc.TotalResources,
				tc.SchedulingConfig.DominantResourceFairnessResourcesToConsider,
			)
			require.NoError(t, err)
			sctx := schedulercontext.NewSchedulingContext(
				"executor",
				"pool",
				tc.SchedulingConfig.Preemption.PriorityClasses,
				tc.SchedulingConfig.Preemption.DefaultPriorityClass,
				fairnessCostProvider,
				tc.TotalResources,
			)
			for queue, priorityFactor := range tc.PriorityFactorByQueue {
				weight := 1 / priorityFactor
				err := sctx.AddQueueSchedulingContext(queue, weight, tc.InitialAllocatedByQueueAndPriorityClass[queue])
				require.NoError(t, err)
			}
			constraints := schedulerconstraints.SchedulingConstraintsFromSchedulingConfig(
				"pool",
				tc.TotalResources,
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
					assert.Equal(t, result.NodeIdByJobId[jctx.JobId], jctx.PodSchedulingContext.NodeId)
				}
				for _, jctx := range qctx.UnsuccessfulJobSchedulingContexts {
					if jctx.PodSchedulingContext != nil {
						assert.Equal(t, "", jctx.PodSchedulingContext.NodeId)
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
					_, _, isGangJob, err := GangIdAndCardinalityFromLegacySchedulerJob(jctx.Job)
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

func NewNodeDb() (*nodedb.NodeDb, error) {
	nodeDb, err := nodedb.NewNodeDb(
		testfixtures.TestPriorityClasses,
		testfixtures.TestMaxExtraNodesToConsider,
		testfixtures.TestResources,
		testfixtures.TestIndexedTaints,
		testfixtures.TestIndexedNodeLabels,
	)
	if err != nil {
		return nil, err
	}
	return nodeDb, nil
}
