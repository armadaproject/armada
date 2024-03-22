package scheduler

import (
	"fmt"
	"testing"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
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
		// Queues
		Queues []queue.Queue
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
			Jobs:                     armadaslices.Concatenate(testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass1, 1), testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: testfixtures.IntRange(0, 0),
		},
		"unschedulable jobs do not block schedulable jobs": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1), testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 10), testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0, 11},
		},
		"MaximumSchedulingBurst": {
			SchedulingConfig: testfixtures.WithGlobalSchedulingRateLimiterConfig(10, 2, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 10),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 4),
			),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0, 11},
		},
		"MaximumPerQueueSchedulingBurst": {
			SchedulingConfig: testfixtures.WithPerQueueSchedulingLimiterConfig(10, 2, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 10),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 3),
				testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 1),
			),
			Queues:                   []queue.Queue{{Name: "A", PriorityFactor: 1.0}, {Name: "B", PriorityFactor: 1.0}},
			ExpectedScheduledIndices: []int{0, 11, 14},
		},
		"MaximumSchedulingBurst is not exceeded by gangs": {
			SchedulingConfig: testfixtures.WithGlobalSchedulingRateLimiterConfig(10, 2, testfixtures.TestSchedulingConfig()),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1),
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
				testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1),
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
			Queues: []queue.Queue{
				{
					Name:           "A",
					PriorityFactor: 1.0,
					ResourceLimitsByPriorityClassName: map[string]api.PriorityClassResourceLimits{
						testfixtures.PriorityClass0: {
							MaximumResourceFractionByPool: map[string]api.PriorityClassPoolResourceLimits{
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
		"fairness two queues": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32), testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass0, 32)),
			Queues:                   []queue.Queue{{Name: "A", PriorityFactor: 1.0}, {Name: "B", PriorityFactor: 1.0}},
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
			Queues: []queue.Queue{{Name: "A", PriorityFactor: 1.0}, {Name: "B", PriorityFactor: 1.0}, {Name: "C", PriorityFactor: 1.0}},
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
			Queues: []queue.Queue{{Name: "A", PriorityFactor: 1.0}, {Name: "B", PriorityFactor: 2.0}},
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
			Queues: []queue.Queue{{Name: "A", PriorityFactor: 1.0}, {Name: "B", PriorityFactor: 2.0}, {Name: "C", PriorityFactor: 10.0}},
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
			Queues: []queue.Queue{{Name: "A", PriorityFactor: 1.0}, {Name: "B", PriorityFactor: 1.0}},
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
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
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
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
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
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0},
		},
		"respect taints": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.NTainted32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1), testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{1},
		},
		"minimum job size": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:             armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1), testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
			Queues:           testfixtures.SingleQueuePriorityOne("A"),
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
			Queues: testfixtures.SingleQueuePriorityOne("A"),
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
			Queues: testfixtures.SingleQueuePriorityOne("A"),
			MinimumJobSize: map[string]resource.Quantity{
				"gpu": resource.MustParse("2"),
			},
			ExpectedScheduledIndices: nil,
		},
		"taints and tolerations": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.NTainted32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1), testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{1},
		},
		"Node selector": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: armadaslices.Concatenate(
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				testfixtures.WithLabelsNodes(map[string]string{"foo": "foo"}, testfixtures.N32CpuNodes(1, testfixtures.TestPriorities)),
			),
			Jobs:                     testfixtures.WithNodeSelectorJobs(map[string]string{"foo": "foo"}, testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 2)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0},
		},
		"taints and tolerations (indexed)": {
			SchedulingConfig:         testfixtures.WithIndexedTaintsConfig([]string{"largeJobsOnly"}, testfixtures.TestSchedulingConfig()),
			Nodes:                    testfixtures.NTainted32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:                     armadaslices.Concatenate(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1), testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{1},
		},
		"Node selector (indexed)": {
			SchedulingConfig: testfixtures.WithIndexedNodeLabelsConfig([]string{"foo"}, testfixtures.TestSchedulingConfig()),
			Nodes: append(
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				testfixtures.WithLabelsNodes(map[string]string{"foo": "foo"}, testfixtures.N32CpuNodes(1, testfixtures.TestPriorities))...,
			),
			Jobs:                     testfixtures.WithNodeSelectorJobs(map[string]string{"foo": "foo"}, testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 2)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
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
			Queues:                        testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices:      []int{0},
			ExpectedNeverAttemptedIndices: []int{3, 4},
		},
		"gang success": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Jobs:                     testfixtures.WithGangAnnotationsJobs(testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 2)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0, 1},
		},
		"non-consecutive gang success": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.WithAnnotationsJobs(map[string]string{
					configuration.GangIdAnnotation:                 "my-gang",
					configuration.GangCardinalityAnnotation:        "2",
					configuration.GangMinimumCardinalityAnnotation: "1",
				},
					testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.WithAnnotationsJobs(map[string]string{
					configuration.GangIdAnnotation:                 "my-gang",
					configuration.GangCardinalityAnnotation:        "2",
					configuration.GangMinimumCardinalityAnnotation: "1",
				},
					testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
			),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0, 1, 2},
		},
		"gang failure": {
			SchedulingConfig:         testfixtures.TestSchedulingConfig(),
			Nodes:                    testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Jobs:                     testfixtures.WithGangAnnotationsJobs(testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 3)),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: nil,
		},
		"non-consecutive gang failure": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Jobs: armadaslices.Concatenate(
				testfixtures.WithAnnotationsJobs(map[string]string{
					configuration.GangIdAnnotation:                 "my-gang",
					configuration.GangCardinalityAnnotation:        "2",
					configuration.GangMinimumCardinalityAnnotation: "2",
				},
					testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.WithAnnotationsJobs(map[string]string{
					configuration.GangIdAnnotation:                 "my-gang",
					configuration.GangCardinalityAnnotation:        "2",
					configuration.GangMinimumCardinalityAnnotation: "2",
				},
					testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)),
			),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
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
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{1},
		},
		"nodeAffinity node notIn": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: armadaslices.Concatenate(
				testfixtures.WithLabelsNodes(
					map[string]string{"key": "val1"},
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				),
				testfixtures.WithLabelsNodes(
					map[string]string{"key": "val2"},
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				),
				testfixtures.WithLabelsNodes(
					map[string]string{"key": "val3"},
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
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
					testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 3),
				),
			),
			Queues:                   testfixtures.SingleQueuePriorityOne("A"),
			ExpectedScheduledIndices: []int{0, 1},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := NewNodeDb(tc.SchedulingConfig)
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

			queueNameToQueue := map[string]*queue.Queue{}
			for _, q := range tc.Queues {
				queueNameToQueue[q.Name] = &q
			}

			indexByJobId := make(map[string]int)
			for i, job := range tc.Jobs {
				if _, ok := queueNameToQueue[job.GetQueue()]; !ok {
					panic(fmt.Sprintf("queue %s does not exist", job.Queue()))
				}
				indexByJobId[job.GetId()] = i
			}
			legacySchedulerJobs := make([]interfaces.LegacySchedulerJob, len(tc.Jobs))
			for i, job := range tc.Jobs {
				legacySchedulerJobs[i] = job
			}
			jobRepo := NewInMemoryJobRepository()
			jobRepo.EnqueueMany(
				schedulercontext.JobSchedulingContextsFromJobs(
					tc.SchedulingConfig.PriorityClasses,
					legacySchedulerJobs,
				),
			)

			fairnessCostProvider, err := fairness.NewDominantResourceFairness(
				tc.TotalResources,
				tc.SchedulingConfig.DominantResourceFairnessResourcesToConsider,
			)
			require.NoError(t, err)
			sctx := schedulercontext.NewSchedulingContext(
				"executor",
				"pool",
				tc.SchedulingConfig.PriorityClasses,
				tc.SchedulingConfig.DefaultPriorityClassName,
				fairnessCostProvider,
				rate.NewLimiter(
					rate.Limit(tc.SchedulingConfig.MaximumSchedulingRate),
					tc.SchedulingConfig.MaximumSchedulingBurst,
				),
				tc.TotalResources,
			)
			for _, q := range tc.Queues {
				weight := 1.0 / float64(q.PriorityFactor)
				err := sctx.AddQueueSchedulingContext(
					q.Name, weight,
					tc.InitialAllocatedByQueueAndPriorityClass[q.Name],
					rate.NewLimiter(
						rate.Limit(tc.SchedulingConfig.MaximumPerQueueSchedulingRate),
						tc.SchedulingConfig.MaximumPerQueueSchedulingBurst,
					),
				)
				require.NoError(t, err)
			}
			constraints := schedulerconstraints.NewSchedulingConstraints(
				"pool",
				tc.TotalResources,
				schedulerobjects.ResourceList{Resources: tc.MinimumJobSize},
				tc.SchedulingConfig,
				tc.Queues,
			)
			jobIteratorByQueue := make(map[string]JobIterator)
			for _, q := range tc.Queues {
				it := jobRepo.GetJobIterator(q.Name)
				jobIteratorByQueue[q.Name] = it
			}
			sch, err := NewQueueScheduler(sctx, constraints, nodeDb, jobIteratorByQueue)
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
				maps.Keys(queueNameToQueue)...,
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
			for _, jctx := range result.ScheduledJobs {
				nodeId, ok := result.NodeIdByJobId[jctx.JobId]
				assert.True(t, ok)
				assert.NotEmpty(t, nodeId)

				node, err := nodeDb.GetNode(nodeId)
				require.NoError(t, err)
				assert.NotEmpty(t, node)

				// Check that the job can actually go onto this node.
				matches, reason, err := nodedb.StaticJobRequirementsMet(node.Taints, node.Labels, node.TotalResources, jctx)
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

func NewNodeDb(config configuration.SchedulingConfig) (*nodedb.NodeDb, error) {
	nodeDb, err := nodedb.NewNodeDb(
		config.PriorityClasses,
		config.MaxExtraNodesToConsider,
		config.IndexedResources,
		config.IndexedTaints,
		config.IndexedNodeLabels,
		config.WellKnownNodeTypes,
	)
	if err != nil {
		return nil, err
	}
	return nodeDb, nil
}
