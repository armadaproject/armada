package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

func TestQueueCandidateGangIterator(t *testing.T) {
	tests := map[string]struct {
		Reqs                   []*schedulerobjects.PodRequirements
		InitialUsageByPriority schedulerobjects.QuantityByPriorityAndResourceType
		SchedulingConstraints  SchedulingConstraints
		// If true, jobs are actually leased.
		LeaseJobs bool
		// Indices of the reqs expected to be returned.
		ExpectedIndices []int
	}{
		"all jobs schedulable": {
			Reqs:            testNSmallCpuJob("A", 0, 3),
			ExpectedIndices: []int{0, 1, 2},
		},
		"minimum job size below limit": {
			Reqs: append(testNSmallCpuJob("A", 0, 3), testNLargeCpuJob("A", 0, 2)...),
			SchedulingConstraints: SchedulingConstraints{
				MinimumJobSize: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("31"),
					},
				},
			},
			ExpectedIndices: []int{3, 4},
		},
		"lookback limit hit": {
			Reqs: testNSmallCpuJob("A", 0, 10),
			SchedulingConstraints: SchedulingConstraints{
				MaxLookbackPerQueue: 4,
			},
			ExpectedIndices: []int{0, 1, 2, 3},
		},
		"minimum job size at limit": {
			Reqs: append(testNSmallCpuJob("A", 0, 3), testNLargeCpuJob("A", 0, 2)...),
			SchedulingConstraints: SchedulingConstraints{
				MinimumJobSize: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
			},
			ExpectedIndices: []int{3, 4},
		},
		"MaximalResourceFractionToSchedulePerQueue": {
			Reqs: testNSmallCpuJob("A", 0, 3),
			SchedulingConstraints: SchedulingConstraints{
				TotalResources: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				MaximalResourceFractionToSchedulePerQueue: armadaresource.ComputeResourcesFloat{
					"cpu": 2.0 / 32.0,
				},
			},
			LeaseJobs:       true,
			ExpectedIndices: []int{0, 1},
		},
		"MaximalResourceFractionPerQueue": {
			Reqs: testNSmallCpuJob("A", 0, 3),
			SchedulingConstraints: SchedulingConstraints{
				TotalResources: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				MaximalResourceFractionPerQueue: armadaresource.ComputeResourcesFloat{
					"cpu": 2.0 / 32.0,
				},
			},
			LeaseJobs:       true,
			ExpectedIndices: []int{0, 1},
		},
		"MaximalResourceFractionPerQueue with initial usage": {
			Reqs: testNSmallCpuJob("A", 0, 3),
			SchedulingConstraints: SchedulingConstraints{
				TotalResources: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				MaximalResourceFractionPerQueue: armadaresource.ComputeResourcesFloat{
					"cpu": 2.0 / 32.0,
				},
			},
			InitialUsageByPriority: schedulerobjects.QuantityByPriorityAndResourceType{
				0: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("1"),
					},
				},
			},
			LeaseJobs:       true,
			ExpectedIndices: []int{0},
		},
		"MaxConsecutiveUnschedulableJobs": {
			Reqs: append(append(testNSmallCpuJob("A", 0, 1), testNGpuJob("A", 0, 3)...), testNSmallCpuJob("A", 0, 1)...),
			SchedulingConstraints: SchedulingConstraints{
				TotalResources: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
						"gpu": resource.MustParse("1"),
					},
				},
				MaximalResourceFractionPerQueue: armadaresource.ComputeResourcesFloat{
					"gpu": 0,
				},
				MaxLookbackPerQueue: 3,
			},
			ExpectedIndices: []int{0},
		},
		"MaximalCumulativeResourceFractionPerQueueAndPriority": {
			Reqs: append(append(testNSmallCpuJob("A", 9, 11), testNSmallCpuJob("A", 7, 11)...), testNSmallCpuJob("A", 3, 11)...),
			SchedulingConstraints: SchedulingConstraints{
				TotalResources: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				MaximalCumulativeResourceFractionPerQueueAndPriority: map[int32]map[string]float64{
					3: {"cpu": 30.0 / 32.0},
					7: {"cpu": 20.0 / 32.0},
					9: {"cpu": 10.0 / 32.0},
				},
			},
			LeaseJobs:       true,
			ExpectedIndices: append(append(intRange(0, 9), intRange(11, 20)...), intRange(22, 31)...),
		},
		"MaximalCumulativeResourceFractionPerQueueAndPriority with initial usage": {
			Reqs: append(append(testNSmallCpuJob("A", 9, 11), testNSmallCpuJob("A", 7, 11)...), testNSmallCpuJob("A", 3, 11)...),
			SchedulingConstraints: SchedulingConstraints{
				TotalResources: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				MaximalCumulativeResourceFractionPerQueueAndPriority: map[int32]map[string]float64{
					3: {"cpu": 30.0 / 32.0},
					7: {"cpu": 20.0 / 32.0},
					9: {"cpu": 10.0 / 32.0},
				},
			},
			InitialUsageByPriority: schedulerobjects.QuantityByPriorityAndResourceType{
				3: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
				7: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("2")}},
				9: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("3")}},
			},
			LeaseJobs:       true,
			ExpectedIndices: append(append(intRange(0, 6), intRange(11, 18)...), intRange(22, 30)...),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			repo := newMockJobRepository()
			jobs := make([]*api.Job, len(tc.Reqs))
			indexByJobId := make(map[string]int)
			for i, req := range tc.Reqs {
				// Queue name doesn't matter.
				jobs[i] = apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
				repo.Enqueue(jobs[i])
				indexByJobId[jobs[i].Id] = i
			}

			expected := make([]*api.Job, len(tc.ExpectedIndices))
			for i, j := range tc.ExpectedIndices {
				expected[i] = jobs[j]
			}

			ctx := context.Background()
			queuedJobsIterator, err := NewQueuedJobsIterator(ctx, "A", repo)
			if !assert.NoError(t, err) {
				return
			}
			queuedGangIterator := NewQueuedGangIterator(
				ctx,
				queuedJobsIterator,
				tc.SchedulingConstraints.MaxLookbackPerQueue,
				configuration.GangIdAnnotation,
				configuration.GangCardinalityAnnotation,
			)
			it := &QueueCandidateGangIterator{
				ctx:                        ctx,
				SchedulingConstraints:      tc.SchedulingConstraints,
				QueueSchedulingRoundReport: NewQueueSchedulingRoundReport(0, tc.InitialUsageByPriority),
				queuedGangIterator:         queuedGangIterator,
			}

			actual := make([]*api.Job, 0)
			actualIndices := make([]int, 0)
			for reports, err := it.Next(); reports != nil; reports, err = it.Next() {
				if !assert.NoError(t, err) {
					return
				}
				for _, report := range reports {
					if tc.LeaseJobs {
						it.QueueSchedulingRoundReport.AddJobSchedulingReport(report, false)
					}
					actual = append(actual, report.Job.(*api.Job))
					actualIndices = append(actualIndices, indexByJobId[report.Job.GetId()])
				}
			}
			assert.Equal(t, tc.ExpectedIndices, actualIndices) // Redundant, but useful to debug tests.
			assert.Equal(t, expected, actual, "")
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
		// Skip checking if reports were generated.
		// Needed for tests where not all jobs are considered.
		DoNotCheckReports bool
		// For each queue, the indices of jobs expected to be scheduled.
		ExpectedIndicesByQueue map[string][]int
		// For each queue, the expected resources assigned to jobs from that queue.
		ExpectedResourcesByQueue map[string]resourceLimits
		// ExpectedJobIndicesByNodeIndex[0]["A"] = [0, 2] indicates that
		// ReqsByQueue["A"][0] and ReqsByQueue["A"][0] expected to land on the 0-th node.
		ExpectedIndicesByNodeIndexAndQueue map[int]map[string][]int
	}{
		"one queue one job": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob("A", 0, 1),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"one queue several jobs": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob("A", 0, 32),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": intRange(0, 31),
			},
			ExpectedIndicesByNodeIndexAndQueue: map[int]map[string][]int{
				0: {"A": intRange(0, 31)},
			},
		},
		"one queue some jobs do not fit": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob("A", 0, 33),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": intRange(0, 31),
			},
		},
		"one queue with jobs of varying size": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(2, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob("A", 0, 32), testNLargeCpuJob("A", 0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": intRange(0, 32),
			},
		},
		"preempt lower-priority jobs": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob("A", 0, 1), testNLargeCpuJob("A", 1, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			// TODO: We could optimise this by un-leasing preempted jobs.
			ExpectedIndicesByQueue: map[string][]int{
				"A": intRange(0, 1),
			},
		},
		"no preemption of higher-priority jobs": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNLargeCpuJob("A", 1, 1), testNSmallCpuJob("A", 0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"unschedulable jobs do not block schedulable jobs": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(append(testNSmallCpuJob("A", 0, 1), testNLargeCpuJob("A", 0, 10)...), testNSmallCpuJob("A", 0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 11},
			},
		},
		"max jobs to schedule limit": {
			SchedulingConfig: withMaxJobsToScheduleConfig(2, testSchedulingConfig()),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob("A", 0, 5),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			DoNotCheckReports: true,
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 1},
			},
		},
		"round limits": {
			SchedulingConfig: withRoundLimitsConfig(map[string]float64{"cpu": 2.0 / 32.0}, testSchedulingConfig()),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob("A", 0, 5),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 1},
			},
		},
		"round per-queue limits": {
			SchedulingConfig: withPerQueueRoundLimitsConfig(map[string]float64{"cpu": 2.0 / 32.0}, testSchedulingConfig()),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob("A", 0, 5),
				"B": testNSmallCpuJob("A", 0, 5),
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
			SchedulingConfig: withPerQueueLimitsConfig(map[string]float64{"cpu": 2.0 / 32.0}, testSchedulingConfig()),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob("A", 0, 5),
				"B": testNSmallCpuJob("A", 0, 5),
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
			SchedulingConfig: withPerQueueLimitsConfig(
				map[string]float64{
					"cpu":    2.0 / 162975640.0,
					"memory": 0.1,
				},
				testSchedulingConfig()),
			Nodes: testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob("A", 0, 5),
				"B": testNSmallCpuJob("A", 0, 5),
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
			SchedulingConfig: withPerQueueLimitsConfig(map[string]float64{"cpu": 2.0 / 32.0}, testSchedulingConfig()),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob("A", 0, 5),
				"B": testNSmallCpuJob("A", 0, 5),
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
			SchedulingConfig: withPerPriorityLimitsConfig(
				map[int32]map[string]float64{
					0: {"cpu": 1.0},
					1: {"cpu": 0.5},
					2: {"cpu": 0.25},
					3: {"cpu": 0.1},
				}, testSchedulingConfig()),
			Nodes: testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob("A", 3, 5), testNSmallCpuJob("A", 0, 5)...),
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
			SchedulingConfig: withPerPriorityLimitsConfig(
				map[int32]map[string]float64{
					0: {"cpu": 0.9}, // 28 cpu
					1: {"cpu": 0.9},
				}, testSchedulingConfig()),
			Nodes: testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob("A", 0, 5), testNSmallCpuJob("A", 0, 5)...),
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
			SchedulingConfig: withPerPriorityLimitsConfig(
				map[int32]map[string]float64{
					0: {"cpu": 0.9}, // 28 cpu
					1: {"cpu": 0.5}, // 14 cpu
				}, testSchedulingConfig()),
			Nodes: testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob("A", 1, 1), testNSmallCpuJob("A", 0, 5)...),
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
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob("A", 0, 32),
				"B": testNSmallCpuJob("A", 0, 32),
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
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob("A", 0, 32),
				"B": testNSmallCpuJob("A", 0, 32),
				"C": testNSmallCpuJob("A", 0, 32),
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
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(3, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob("A", 0, 96),
				"B": testNSmallCpuJob("A", 0, 96),
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
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(3, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob("A", 0, 96),
				"B": testNSmallCpuJob("A", 0, 96),
				"C": testNSmallCpuJob("A", 0, 96),
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
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob("A", 0, 32),
				"B": testNSmallCpuJob("A", 0, 32),
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
		"Node with no available capacity": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes: withUsedResourcesNodes(
				0,
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				testNCpuNode(1, testPriorities),
			),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob("A", 0, 1),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": nil,
			},
		},
		"Node with some available capacity": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes: withUsedResourcesNodes(
				0,
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("31"),
					},
				},
				testNCpuNode(1, testPriorities),
			),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNSmallCpuJob("A", 0, 2),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"preempt used resources of lower-priority jobs": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes: withUsedResourcesNodes(
				0,
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("32"),
					},
				},
				testNCpuNode(1, testPriorities),
			),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": testNLargeCpuJob("A", 1, 1),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"respect taints": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNTaintedCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob("A", 0, 1), testNLargeCpuJob("A", 0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {1},
			},
		},
		"minimum job size": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob("A", 0, 1), testNLargeCpuJob("A", 0, 1)...),
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
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNGpuNode(2, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(append(testNSmallCpuJob("A", 0, 1), testNLargeCpuJob("A", 0, 1)...), testNGpuJob("A", 0, 1)...),
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
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNGpuNode(2, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(append(testNSmallCpuJob("A", 0, 1), testNLargeCpuJob("A", 0, 1)...), testNGpuJob("A", 0, 1)...),
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
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNTaintedCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob("A", 0, 1), testNLargeCpuJob("A", 0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {1},
			},
		},
		"Node selector": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes: append(
				testNCpuNode(1, testPriorities),
				withLabelsNodes(map[string]string{"foo": "foo"}, testNCpuNode(1, testPriorities))...,
			),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": withNodeSelectorPodReqs(map[string]string{"foo": "foo"}, testNLargeCpuJob("A", 0, 2)),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"taints and tolerations (indexed)": {
			SchedulingConfig: withIndexedTaintsConfig([]string{"largeJobsOnly"}, testSchedulingConfig()),
			Nodes:            testNTaintedCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(testNSmallCpuJob("A", 0, 1), testNLargeCpuJob("A", 0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {1},
			},
		},
		"Node selector (indexed)": {
			SchedulingConfig: withIndexedNodeLabelsConfig([]string{"foo"}, testSchedulingConfig()),
			Nodes: append(
				testNCpuNode(1, testPriorities),
				withLabelsNodes(map[string]string{"foo": "foo"}, testNCpuNode(1, testPriorities))...,
			),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": withNodeSelectorPodReqs(map[string]string{"foo": "foo"}, testNLargeCpuJob("A", 0, 2)),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"QueueLeaseBatchSize Respected": {
			SchedulingConfig: withQueueLeaseBatchSizeConfig(3, testSchedulingConfig()), // should quit after 3 unschedulable jobs
			Nodes:            testNCpuNode(1, testPriorities),                          // 32 cores
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(append(testNSmallCpuJob("A", 0, 1), testNLargeCpuJob("A", 0, 3)...), testNSmallCpuJob("A", 0, 1)...),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0},
			},
		},
		"gang scheduling success": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(2, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": withGangAnnotationsPodReqs(testNLargeCpuJob("A", 0, 2)),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {0, 1},
			},
		},
		"gang scheduling failure": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(2, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": withGangAnnotationsPodReqs(testNLargeCpuJob("A", 0, 3)),
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
			ExpectedIndicesByQueue: map[string][]int{
				"A": {},
			},
		},
		"gang aggregated resource accounting": {
			SchedulingConfig: withPerQueueLimitsConfig(
				map[string]float64{
					"cpu": 2.0 / 32.0,
				},
				testSchedulingConfig(),
			),
			Nodes: testNCpuNode(1, testPriorities),
			ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
				"A": append(append(
					withAnnotationsPodReqs(map[string]string{configuration.GangIdAnnotation: "my-gang", configuration.GangCardinalityAnnotation: "2"}, testNSmallCpuJob("A", 0, 1)),
					testNSmallCpuJob("A", 0, 1)...),
					withAnnotationsPodReqs(map[string]string{configuration.GangIdAnnotation: "my-gang", configuration.GangCardinalityAnnotation: "2"}, testNSmallCpuJob("A", 0, 1))...,
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
			jobRepository := newMockJobRepository()
			for queue, reqs := range tc.ReqsByQueue {
				jobRepository.EnqueueMany(apiJobsFromPodReqs(queue, reqs))
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

			nodeDb, err := createNodeDb(tc.Nodes)
			if !assert.NoError(t, err) {
				return
			}

			// If not provided, set total resources equal to the aggregate over tc.Nodes.
			if tc.TotalResources.Resources == nil {
				tc.TotalResources = nodeDb.totalResources.DeepCopy()
			}

			constraints := SchedulingConstraintsFromSchedulingConfig(
				"executor",
				"pool",
				schedulerobjects.ResourceList{Resources: tc.MinimumJobSize},
				tc.SchedulingConfig,
				tc.TotalResources,
			)
			queues := make([]*Queue, 0, len(tc.PriorityFactorByQueue))
			for name, priorityFactor := range tc.PriorityFactorByQueue {
				jobIterator, err := jobRepository.GetJobIterator(context.Background(), name)
				require.NoError(t, err)
				queue, err := NewQueue(name, priorityFactor, jobIterator)
				require.NoError(t, err)
				queues = append(queues, queue)
			}
			sched, err := NewLegacyScheduler(
				context.Background(),
				*constraints,
				tc.SchedulingConfig,
				nodeDb,
				queues,
				tc.InitialUsageByQueue,
			)
			if !assert.NoError(t, err) {
				return
			}

			jobs, err := sched.Schedule()
			if !assert.NoError(t, err) {
				return
			}

			// Check that the right jobs got scheduled.
			if tc.ExpectedIndicesByQueue != nil {
				actualScheduledJobsByQueue := jobIdsByQueueFromJobs(jobs)
				for queue, expected := range expectedByQueue {
					actual, ok := actualScheduledJobsByQueue[queue]
					if !ok {
						// Since expected is []string{} if we expect no jobs to be scheduled.
						actual = make([]string, 0)
					}
					assert.Equal(t, expected, actual, "queue %s", queue)
				}
			}

			// Check that each queue was allocated the right amount of resources.
			if tc.ExpectedResourcesByQueue != nil {
				actualUsageByQueue := usageByQueue(jobs, tc.SchedulingConfig.Preemption.PriorityClasses)
				for queue, usage := range actualUsageByQueue {
					assertResourceLimitsSatisfied(t, tc.ExpectedResourcesByQueue[queue], usage)
				}
			}

			// Check that a scheduling round report was created.
			if !assert.NotNil(t, sched.SchedulingRoundReport) {
				return
			}

			// Check that scheduling reports were generated.
			// TODO: Check that reports correctly indicate success/not.
			if !tc.DoNotCheckReports {
				schedulingRoundReport := sched.SchedulingRoundReport

				// Check that started and finished it set.
				assert.NotEqual(t, time.Time{}, schedulingRoundReport.Started)
				assert.NotEqual(t, time.Time{}, schedulingRoundReport.Finished)

				// Check that scheduling round report scheduled resources is set correctly.
				for queue, expected := range usageByQueueAndPriority(jobs, tc.SchedulingConfig.Preemption.PriorityClasses) {
					queueSchedulingRoundReport, ok := schedulingRoundReport.QueueSchedulingRoundReports[queue]
					if !assert.NotNil(t, queueSchedulingRoundReport) {
						continue
					}
					if !assert.True(t, ok) {
						continue
					}
					actual := queueSchedulingRoundReport.ScheduledResourcesByPriority
					assert.True(t, expected.Equal(actual))
				}

				// Check that the scheduling round report contains reports for all queues and jobs.
				assert.Equal(
					t,
					len(tc.PriorityFactorByQueue),
					len(sched.SchedulingRoundReport.QueueSchedulingRoundReports),
				)
				leasedJobIds := make(map[uuid.UUID]interface{})
				for _, job := range jobs {
					jobId, err := uuidFromUlidString(job.GetId())
					if !assert.NoError(t, err) {
						return
					}
					leasedJobIds[jobId] = true
				}
				for queue, jobs := range jobRepository.jobsByQueue {
					queueSchedulingRoundReport, ok := schedulingRoundReport.QueueSchedulingRoundReports[queue]
					if !assert.NotNil(t, queueSchedulingRoundReport) {
						continue
					}
					if !assert.True(t, ok) {
						continue
					}

					for i, job := range jobs {
						if i >= int(tc.SchedulingConfig.QueueLeaseBatchSize) {
							break
						}
						jobId, err := uuidFromUlidString(job.Id)
						if !assert.NoError(t, err) {
							return
						}

						_, isLeased := leasedJobIds[jobId]
						var jobReports map[uuid.UUID]*JobSchedulingReport
						if isLeased {
							jobReports = queueSchedulingRoundReport.SuccessfulJobSchedulingReports
						} else {
							jobReports = queueSchedulingRoundReport.UnsuccessfulJobSchedulingReports
						}
						if !assert.NotNil(t, jobReports) {
							continue
						}

						jobReport, ok := jobReports[jobId]
						if !assert.True(t, ok, "missing report for job; leased: %v", isLeased) {
							continue
						}
						if !assert.NotNil(t, jobReport) {
							continue
						}
					}
				}

				// Check that total resources is correct.
				assert.True(t, schedulingRoundReport.TotalResources.Equal(tc.TotalResources))

				// Check that we were given a termination reason.
				assert.NotEmpty(t, schedulingRoundReport.TerminationReason)
			}
		})
	}
}

func TestReschedule(t *testing.T) {
	type ReschedulingRound struct {
		// Map from queue name to pod requirements for that queue.
		ReqsByQueue map[string][]*schedulerobjects.PodRequirements
		// For each queue, indices jobs expected to be scheduled.
		ExpectedScheduledIndices map[string][]int
		// For each queue, indices of jobs expected to be preempted.
		// E.g., ExpectedPreemptedIndices["A"][0] is the indices of jobs declared for queue A in round 0.
		ExpectedPreemptedIndices map[string]map[int][]int
	}
	tests := map[string]struct {
		SchedulingConfig configuration.SchedulingConfig
		// Nodes to be considered by the scheduler.
		Nodes []*schedulerobjects.Node
		// Each item corresponds to a call to Reschedule().
		Rounds []ReschedulingRound
		// Map from queue to the priority factor associated with that queue.
		PriorityFactorByQueue map[string]float64
		// Initial resource usage for all queues.
		// This value is used across all rounds,
		// i.e., we don't update it based on preempted/scheduled jobs.
		InitialUsageByQueue map[string]schedulerobjects.QuantityByPriorityAndResourceType
		// Total resources across all clusters.
		// If empty, it is computed as the total resources across the provided nodes.
		TotalResources schedulerobjects.ResourceList
		// Minimum job size.
		MinimumJobSize map[string]resource.Quantity
	}{
		"balancing three queues": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 31),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"B": testNSmallCpuJob("A", 0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": intRange(0, 15),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: intRange(16, 31),
						},
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"C": testNSmallCpuJob("A", 0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"C": intRange(0, 9),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: intRange(11, 15),
						},
						"B": {
							1: intRange(11, 15),
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
		"balancing two queues weighted": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 31),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"B": testNSmallCpuJob("A", 0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": intRange(0, 20),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: intRange(11, 31),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 2,
				"B": 1,
			},
		},
		"balancing two queues weighted with inactive queues": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 31),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"B": testNSmallCpuJob("A", 0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": intRange(0, 20),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: intRange(11, 31),
						},
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
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(2, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 31),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"B": testNSmallCpuJob("B", 0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": intRange(0, 31),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"reschedule onto same node with PC preemption": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(2, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 31),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 1, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 31),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: intRange(0, 31),
						},
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 31),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"reschedule onto same node reverse order": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(2, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"B": testNSmallCpuJob("B", 0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": intRange(0, 31),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 31),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"rescheduled jobs don't count towards maxJobsToSchedule": {
			SchedulingConfig: withMaxJobsToScheduleConfig(5, testSchedulingConfig()),
			Nodes:            testNCpuNode(1, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 4),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 4),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"rescheduled jobs don't count towards maxLookbackPerQueue": {
			SchedulingConfig: withMaxLookbackPerQueueConfig(5, testSchedulingConfig()),
			Nodes:            testNCpuNode(1, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 4),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 4),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"rescheduled jobs don't count towards MaximalResourceFractionToSchedulePerQueue": {
			SchedulingConfig: withPerQueueRoundLimitsConfig(
				map[string]float64{
					"cpu": 5.0 / 32.0,
				},
				testSchedulingConfig(),
			),
			Nodes: testNCpuNode(1, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 4),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 4),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"rescheduled jobs don't count towards MaximalClusterFractionToSchedule": {
			SchedulingConfig: withRoundLimitsConfig(
				map[string]float64{
					"cpu": 5.0 / 32.0,
				},
				testSchedulingConfig(),
			),
			Nodes: testNCpuNode(1, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 4),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 10),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 4),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"priority class preemption two classes": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNLargeCpuJob("A", 0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 0),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNLargeCpuJob("A", 1, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 0),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: intRange(0, 0),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"priority class preemption cross-queue": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNLargeCpuJob("A", 0, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 0),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"B": testNLargeCpuJob("B", 1, 1),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": intRange(0, 0),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: intRange(0, 0),
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
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": append(testNLargeCpuJob("A", 0, 1), testNLargeCpuJob("A", 1, 1)...),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(1, 1),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"priority class preemption four classes": {
			SchedulingConfig: testSchedulingConfig(),
			Nodes:            testNCpuNode(1, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": append(append(
							testNSmallCpuJob("A", 0, 10),
							testNSmallCpuJob("A", 1, 10)...),
							testNSmallCpuJob("A", 2, 10)...,
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 29),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 3, 24),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 23),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: append(intRange(0, 19), intRange(28, 29)...),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"per-priority class limits": {
			SchedulingConfig: withPerPriorityLimitsConfig(
				map[int32]map[string]float64{
					0: {"cpu": 60.0 / 64.0},
					1: {"cpu": 20.0 / 64.0},
				},
				testSchedulingConfig(),
			),
			Nodes: testNCpuNode(2, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": append(
							testNSmallCpuJob("A", 1, 64),
							testNSmallCpuJob("A", 0, 64)...,
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": append(intRange(0, 19), intRange(64, 103)...),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 1),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"per-priority class limits multiple rounds": {
			SchedulingConfig: withPerPriorityLimitsConfig(
				map[int32]map[string]float64{
					0: {"cpu": 30.0 / 32.0},
					1: {"cpu": 10.0 / 32.0},
				},
				testSchedulingConfig(),
			),
			Nodes: testNCpuNode(1, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": append(
							testNSmallCpuJob("A", 1, 5),
							testNSmallCpuJob("A", 0, 10)...,
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 14),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": append(
							testNSmallCpuJob("A", 1, 32),
							testNSmallCpuJob("A", 0, 32)...,
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": append(intRange(0, 4), intRange(32, 41)...),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": append(
							testNSmallCpuJob("A", 1, 32),
							testNSmallCpuJob("A", 0, 32)...,
						),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"MaximalClusterFractionToSchedule": {
			SchedulingConfig: withRoundLimitsConfig(
				map[string]float64{"cpu": 0.25},
				testSchedulingConfig(),
			),
			Nodes: testNCpuNode(2, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"B": testNSmallCpuJob("B", 0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": intRange(0, 15),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 64),
						"B": testNSmallCpuJob("B", 0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 15),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 64),
						"B": testNSmallCpuJob("B", 0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 7),
						"B": intRange(0, 7),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 64),
						"B": testNSmallCpuJob("B", 0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 7),
						"B": intRange(0, 7),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
				"B": 1,
			},
		},
		"MaximalResourceFractionToSchedulePerQueue": {
			SchedulingConfig: withPerQueueRoundLimitsConfig(
				map[string]float64{"cpu": 0.25},
				testSchedulingConfig(),
			),
			Nodes: testNCpuNode(2, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 15),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 15),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 15),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 64),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 15),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 64),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
		"MaximalResourceFractionPerQueue": {
			SchedulingConfig: withPerQueueLimitsConfig(
				map[string]float64{"cpu": 0.5},
				testSchedulingConfig(),
			),
			Nodes: testNCpuNode(1, testPriorities),
			Rounds: []ReschedulingRound{
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": intRange(0, 15),
					},
				},
				{
					ReqsByQueue: map[string][]*schedulerobjects.PodRequirements{
						"A": testNSmallCpuJob("A", 0, 32),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{
				"A": 1,
			},
		},
	}
	for name, tc := range tests {
		// All tests are for eviction probability of 1.
		tc.SchedulingConfig.Preemption.NodeEvictionProbability = 1
		tc.SchedulingConfig.Preemption.NodeOversubscriptionEvictionProbability = 1
		t.Run(name, func(t *testing.T) {
			nodeDb, err := createNodeDb(tc.Nodes)
			require.NoError(t, err)
			repo := NewInMemoryJobRepository(testPriorityClasses)
			roundByJobId := make(map[string]int)
			indexByJobId := make(map[string]int)
			initialUsageByQueue := armadamaps.DeepCopy(tc.InitialUsageByQueue)
			expectedNodeIdByJobId := make(map[string]string)
			log := logrus.NewEntry(logrus.New())
			for i, round := range tc.Rounds {
				fmt.Println("====== starting round", i)
				jobs := make([]LegacySchedulerJob, 0)
				for queue, reqs := range round.ReqsByQueue {
					// TODO: Remove PC name argument. Since we now infer it.
					jobs = append(jobs, legacySchedulerJobsFromPodReqs(queue, "", reqs)...)
				}
				repo.jobsByQueue = make(map[string][]LegacySchedulerJob)
				repo.EnqueueMany(jobs)
				for _, reqs := range round.ReqsByQueue {
					for j, req := range reqs {
						jobId, err := JobIdFromPodRequirements(req)
						require.NoError(t, err)
						roundByJobId[jobId] = i
						indexByJobId[jobId] = j
					}
				}

				// If not provided, set total resources equal to the aggregate over tc.Nodes.
				if tc.TotalResources.Resources == nil {
					tc.TotalResources = nodeDb.totalResources.DeepCopy()
				}

				constraints := SchedulingConstraintsFromSchedulingConfig(
					"executor",
					"pool",
					schedulerobjects.ResourceList{Resources: tc.MinimumJobSize},
					tc.SchedulingConfig,
					tc.TotalResources,
				)
				rescheduler := NewRescheduler(
					*constraints,
					tc.SchedulingConfig,
					repo,
					nodeDb,
					tc.PriorityFactorByQueue,
					initialUsageByQueue,
					nil,
				)
				result, err := rescheduler.Schedule(ctxlogrus.ToContext(context.Background(), log))
				// preemptedJobs, scheduledJobs, nodesByJobId, updatedUsageByQueue, err := Reschedule(
				// 	ctxlogrus.ToContext(context.Background(), log),
				// 	repo,
				// 	*constraints,
				// 	tc.SchedulingConfig,
				// 	nodeDb,
				// 	tc.PriorityFactorByQueue,
				// 	initialUsageByQueue,
				// 	1, 1,
				// 	nil,
				// )
				require.NoError(t, err)

				// Update initialUsage.
				for _, job := range result.PreemptedJobs {
					req := PodRequirementFromLegacySchedulerJob(job, tc.SchedulingConfig.Preemption.PriorityClasses)
					requests := schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests)
					quantityByPriorityAndResourceType := schedulerobjects.QuantityByPriorityAndResourceType{
						req.Priority: requests,
					}
					initialUsageByQueue[job.GetQueue()].Sub(quantityByPriorityAndResourceType)
				}
				for _, job := range result.ScheduledJobs {
					req := PodRequirementFromLegacySchedulerJob(job, tc.SchedulingConfig.Preemption.PriorityClasses)
					requests := schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests)
					quantityByPriorityAndResourceType := schedulerobjects.QuantityByPriorityAndResourceType{
						req.Priority: requests,
					}
					m := initialUsageByQueue[job.GetQueue()]
					if m == nil {
						m = make(schedulerobjects.QuantityByPriorityAndResourceType)
					}
					m.Add(quantityByPriorityAndResourceType)
					initialUsageByQueue[job.GetQueue()] = m
				}
				assert.Equal(t, initialUsageByQueue, result.UsageByQueueAndPriority)

				// Test that jobs are mapped to nodes correctly.
				for _, job := range result.PreemptedJobs {
					node, ok := result.NodeByJobId[job.GetId()]
					assert.True(t, ok)
					assert.NotNil(t, node)

					// Check that preempted jobs are preempted from the node they were previously scheduled onto.
					nodeId, ok := expectedNodeIdByJobId[job.GetId()]
					assert.True(t, ok)
					assert.Equal(t, nodeId, node.Id, "job %s preempted from unexpected node", job.GetId())
				}
				for _, job := range result.ScheduledJobs {
					node, ok := result.NodeByJobId[job.GetId()]
					assert.True(t, ok)
					assert.NotNil(t, node)

					// Check that scheduled jobs are consistently assigned to the same node.
					// (We don't allow moving jobs between nodes.)
					if nodeId, ok := expectedNodeIdByJobId[job.GetId()]; ok {
						assert.Equal(t, nodeId, node.Id, "job %s scheduled onto unexpected node", job.GetId())
					} else {
						expectedNodeIdByJobId[job.GetId()] = node.Id
					}
				}
				for jobId, node := range result.NodeByJobId {
					if nodeId, ok := expectedNodeIdByJobId[jobId]; ok {
						assert.Equal(t, nodeId, node.Id, "job %s preempted from/scheduled onto unexpected node", jobId)
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
				prioritiesByName := configuration.PriorityByPriorityClassName(testPriorityClasses)
				priorities := maps.Values(prioritiesByName)
				slices.Sort(priorities)
				it, err := NewNodesIterator(nodeDb.Txn(false))
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

func BenchmarkReschedule(b *testing.B) {
	tests := map[string]struct {
		SchedulingConfig  configuration.SchedulingConfig
		Nodes             []*schedulerobjects.Node
		PodReqFunc        func(queue string, priority int32, n int) []*schedulerobjects.PodRequirements
		NumQueues         int
		NumJobsPerQueue   int
		MinimumJobSize    map[string]resource.Quantity
		MinPriorityFactor int
		MaxPriorityFactor int
	}{
		"1 node 1 queue 32 jobs": {
			SchedulingConfig: withNodeEvictionProbabilityConfig(
				0.1,
				testSchedulingConfig(),
			),
			Nodes:             testNCpuNode(1, testPriorities),
			PodReqFunc:        testNSmallCpuJob,
			NumQueues:         1,
			NumJobsPerQueue:   32,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 1,
		},
		"10 nodes 1 queue 320 jobs": {
			SchedulingConfig: withNodeEvictionProbabilityConfig(
				0.1,
				testSchedulingConfig(),
			),
			Nodes:             testNCpuNode(10, testPriorities),
			PodReqFunc:        testNSmallCpuJob,
			NumQueues:         1,
			NumJobsPerQueue:   320,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 1,
		},
		"100 nodes 1 queue 3200 jobs": {
			SchedulingConfig: withNodeEvictionProbabilityConfig(
				0.1,
				testSchedulingConfig(),
			),
			Nodes:             testNCpuNode(100, testPriorities),
			PodReqFunc:        testNSmallCpuJob,
			NumQueues:         1,
			NumJobsPerQueue:   3200,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 1,
		},
		"1000 nodes 1 queue 32000 jobs": {
			SchedulingConfig: withNodeEvictionProbabilityConfig(
				0.1,
				testSchedulingConfig(),
			),
			Nodes:             testNCpuNode(1000, testPriorities),
			PodReqFunc:        testNSmallCpuJob,
			NumQueues:         1,
			NumJobsPerQueue:   32000,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 1,
		},
		"mixed": {
			SchedulingConfig: withNodeEvictionProbabilityConfig(
				0.1,
				testSchedulingConfig(),
			),
			Nodes:             testNCpuNode(500, testPriorities),
			PodReqFunc:        testNSmallCpuJob,
			NumQueues:         100,
			NumJobsPerQueue:   256,
			MinPriorityFactor: 1,
			MaxPriorityFactor: 10,
		},
	}
	for name, tc := range tests {
		b.Run(name, func(b *testing.B) {
			reqsByQueue := make(map[string][]*schedulerobjects.PodRequirements)
			priorityFactorByQueue := make(map[string]float64)
			for i := 0; i < tc.NumQueues; i++ {
				queue := fmt.Sprintf("%d", i)
				reqsByQueue[queue] = tc.PodReqFunc(queue, 0, tc.NumJobsPerQueue)
				priorityFactorByQueue[queue] = float64(rand.Intn(tc.MaxPriorityFactor-tc.MinPriorityFactor+1) + tc.MinPriorityFactor)
			}

			nodeDb, err := createNodeDb(tc.Nodes)
			require.NoError(b, err)
			repo := NewInMemoryJobRepository(testPriorityClasses)
			usageByQueue := make(map[string]schedulerobjects.QuantityByPriorityAndResourceType)

			jobs := make([]LegacySchedulerJob, 0)
			for queue, reqs := range reqsByQueue {
				jobs = append(jobs, legacySchedulerJobsFromPodReqs(queue, "", reqs)...)
			}
			repo.EnqueueMany(jobs)

			constraints := SchedulingConstraintsFromSchedulingConfig(
				"executor",
				"pool",
				schedulerobjects.ResourceList{Resources: tc.MinimumJobSize},
				tc.SchedulingConfig,
				nodeDb.totalResources,
			)
			rescheduler := NewRescheduler(
				*constraints,
				tc.SchedulingConfig,
				repo, nodeDb,
				priorityFactorByQueue,
				usageByQueue,
				nil,
			)
			result, err := rescheduler.Schedule(
				ctxlogrus.ToContext(
					context.Background(),
					logrus.NewEntry(logrus.New()),
				),
			)
			require.NoError(b, err)
			require.Equal(b, 0, len(result.PreemptedJobs))

			// Create a new job repo without the scheduled jobs.
			scheduledJobsById := make(map[string]LegacySchedulerJob)
			for _, job := range result.ScheduledJobs {
				scheduledJobsById[job.GetId()] = job
			}
			unscheduledJobs := make([]LegacySchedulerJob, 0)
			for _, job := range jobs {
				if _, ok := scheduledJobsById[job.GetId()]; !ok {
					unscheduledJobs = append(unscheduledJobs, job)
				}
			}
			repo = NewInMemoryJobRepository(testPriorityClasses)
			repo.EnqueueMany(unscheduledJobs)

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				rescheduler := NewRescheduler(
					*constraints,
					tc.SchedulingConfig,
					repo, nodeDb,
					priorityFactorByQueue,
					usageByQueue,
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

func TestPodRequirementFromLegacySchedulerJob(t *testing.T) {
	resourceLimit := v1.ResourceList{
		"cpu":               resource.MustParse("1"),
		"memory":            resource.MustParse("128Mi"),
		"ephemeral-storage": resource.MustParse("8Gi"),
	}
	requirements := v1.ResourceRequirements{
		Limits:   resourceLimit,
		Requests: resourceLimit,
	}

	j := &api.Job{
		Id:       util.NewULID(),
		Queue:    "test",
		JobSetId: "set1",
		Priority: 1,
		Annotations: map[string]string{
			"something":                             "test",
			configuration.GangIdAnnotation:          "gang-id",
			configuration.GangCardinalityAnnotation: "1",
		},
		PodSpecs: []*v1.PodSpec{
			{
				Containers: []v1.Container{
					{
						Resources: requirements,
					},
				},
				PriorityClassName: "armada-default",
			},
		},
	}

	expected := &schedulerobjects.PodRequirements{
		Priority:             1,
		PreemptionPolicy:     string(v1.PreemptLowerPriority),
		ResourceRequirements: requirements,
		Annotations: map[string]string{
			configuration.GangIdAnnotation:          "gang-id",
			configuration.GangCardinalityAnnotation: "1",
			JobIdAnnotation:                         j.Id,
			QueueAnnotation:                         j.Queue,
		},
	}

	result := PodRequirementFromLegacySchedulerJob(j, map[string]configuration.PriorityClass{"armada-default": {Priority: int32(1)}})

	assert.Equal(t, expected, result)
}

func TestEvictOversubscribed(t *testing.T) {
	nodes := testNCpuNode(1, testPriorities)
	node := nodes[0]
	var err error
	jobs := append(
		legacySchedulerJobsFromPodReqs("A", "priority-0", testNSmallCpuJob("A", 0, 20)),
		legacySchedulerJobsFromPodReqs("A", "priority-1", testNSmallCpuJob("A", 1, 20))...,
	)
	reqs := PodRequirementsFromLegacySchedulerJobs(jobs, testPriorityClasses)
	for _, req := range reqs {
		node, err = BindPodToNode(req, node)
		require.NoError(t, err)
	}
	nodes[0] = node

	jobRepo := NewInMemoryJobRepository(testPriorityClasses)
	jobRepo.EnqueueMany(jobs)
	evictor := NewOversubscribedEvictor(
		jobRepo,
		testPriorityClasses,
		1,
	)
	it := NewInMemoryNodeIterator(nodes)
	result, err := evictor.Evict(context.Background(), it)
	require.NoError(t, err)

	prioritiesByName := configuration.PriorityByPriorityClassName(testPriorityClasses)
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

func apiJobsFromPodReqs(queue string, reqs []*schedulerobjects.PodRequirements) []*api.Job {
	rv := make([]*api.Job, len(reqs))
	for i, req := range reqs {
		rv[i] = apiJobFromPodSpec(queue, podSpecFromPodRequirements(req))
		rv[i].Annotations = maps.Clone(req.Annotations)
	}
	return rv
}

func legacySchedulerJobsFromPodReqs(queue, priorityClassName string, reqs []*schedulerobjects.PodRequirements) []LegacySchedulerJob {
	rv := make([]LegacySchedulerJob, len(reqs))
	T := time.Now()
	// TODO: This only works if each PC has a unique priority.
	priorityClassNameByPriority := make(map[int32]string)
	for priorityClassName, priorityClass := range testPriorityClasses {
		if _, ok := priorityClassNameByPriority[priorityClass.Priority]; ok {
			panic(fmt.Sprintf("duplicate priority %d", priorityClass.Priority))
		}
		priorityClassNameByPriority[priorityClass.Priority] = priorityClassName
	}
	for i, req := range reqs {
		// TODO: Let's find a better way to pass around PCs. And for setting, e.g., created.
		podSpec := podSpecFromPodRequirements(req)
		priorityClassName := priorityClassNameByPriority[*podSpec.Priority]
		if priorityClassName == "" {
			panic(fmt.Sprintf("no priority class with priority %d", *podSpec.Priority))
		}
		podSpec.PriorityClassName = priorityClassName
		job := apiJobFromPodSpec(queue, podSpec)
		job.Annotations = maps.Clone(req.Annotations)
		job.Created = T.Add(time.Duration(i) * time.Second)
		if jobId := job.Annotations[JobIdAnnotation]; jobId != "" {
			job.Id = jobId
		}
		rv[i] = job
	}
	return rv
}

type resourceLimits struct {
	Minimum schedulerobjects.ResourceList
	Maximum schedulerobjects.ResourceList
}

func newResourceLimits(minimum map[string]resource.Quantity, maximum map[string]resource.Quantity) resourceLimits {
	return resourceLimits{
		Minimum: schedulerobjects.ResourceList{Resources: minimum},
		Maximum: schedulerobjects.ResourceList{Resources: maximum},
	}
}

func assertResourceLimitsSatisfied(t *testing.T, limits resourceLimits, resources schedulerobjects.ResourceList) bool {
	for resource, min := range limits.Minimum.Resources {
		actual := resources.Resources[resource]
		if !assert.NotEqual(t, 1, min.Cmp(actual), "%s limits not satisfied: min is %s, but actual is %s", resource, min.String(), actual.String()) {
			return false
		}
	}
	for resource, actual := range resources.Resources {
		if max, ok := limits.Maximum.Resources[resource]; ok {
			if !assert.NotEqual(t, -1, max.Cmp(actual), "%s limits not satisfied: max is %s, but actual is %s", resource, max.String(), actual.String()) {
				return false
			}
		}
	}
	return true
}

func jobIdsByQueueFromJobs(jobs []LegacySchedulerJob) map[string][]string {
	rv := make(map[string][]string)
	for _, job := range jobs {
		rv[job.GetQueue()] = append(rv[job.GetQueue()], job.GetId())
	}
	return rv
}

func usageByQueue(jobs []LegacySchedulerJob, priorityClasses map[string]configuration.PriorityClass) map[string]schedulerobjects.ResourceList {
	rv := make(map[string]schedulerobjects.ResourceList)
	for queue, quantityByPriorityAndResourceType := range usageByQueueAndPriority(jobs, priorityClasses) {
		rv[queue] = quantityByPriorityAndResourceType.AggregateByResource()
	}
	return rv
}

func usageByQueueAndPriority(jobs []LegacySchedulerJob, priorityByPriorityClassName map[string]configuration.PriorityClass) map[string]schedulerobjects.QuantityByPriorityAndResourceType {
	rv := make(map[string]schedulerobjects.QuantityByPriorityAndResourceType)
	for _, job := range jobs {
		m, ok := rv[job.GetQueue()]
		if !ok {
			m = make(schedulerobjects.QuantityByPriorityAndResourceType)
			rv[job.GetQueue()] = m
		}
		priority := PodRequirementFromJobSchedulingInfo(job.GetRequirements(priorityByPriorityClassName)).Priority
		rl, ok := m[priority]
		if !ok {
			rl.Resources = make(map[string]resource.Quantity)
		}
		rl.Add(job.GetRequirements(priorityByPriorityClassName).GetTotalResourceRequest())
		m[priority] = rl
	}
	return rv
}

func apiJobFromPodSpec(queue string, podSpec *v1.PodSpec) *api.Job {
	return &api.Job{
		Id:      util.NewULID(),
		PodSpec: podSpec,
		Queue:   queue,
	}
}

func podSpecFromPodRequirements(req *schedulerobjects.PodRequirements) *v1.PodSpec {
	return &v1.PodSpec{
		NodeSelector:     req.NodeSelector,
		Affinity:         req.Affinity,
		Tolerations:      req.Tolerations,
		Priority:         &req.Priority,
		PreemptionPolicy: (*v1.PreemptionPolicy)(&req.PreemptionPolicy),
		Containers: []v1.Container{
			{
				Resources: req.ResourceRequirements,
			},
		},
	}
}

type mockJobRepository struct {
	jobsByQueue map[string][]*api.Job
	jobsById    map[string]*api.Job
	// Ids of all jobs hat were leased to an executor.
	leasedJobs          map[string]bool
	getQueueJobIdsDelay time.Duration
}

func newMockJobRepository() *mockJobRepository {
	return &mockJobRepository{
		jobsByQueue: make(map[string][]*api.Job),
		jobsById:    make(map[string]*api.Job),
		leasedJobs:  make(map[string]bool),
	}
}

func (repo *mockJobRepository) EnqueueMany(jobs []*api.Job) {
	for _, job := range jobs {
		repo.Enqueue(job)
	}
}

func (repo *mockJobRepository) Enqueue(job *api.Job) {
	repo.jobsByQueue[job.Queue] = append(repo.jobsByQueue[job.Queue], job)
	repo.jobsById[job.Id] = job
}

func (repo *mockJobRepository) GetJobIterator(ctx context.Context, queue string) (JobIterator, error) {
	return NewQueuedJobsIterator(ctx, queue, repo)
}

func (repo *mockJobRepository) GetQueueJobIds(queue string) ([]string, error) {
	time.Sleep(repo.getQueueJobIdsDelay)
	if jobs, ok := repo.jobsByQueue[queue]; ok {
		rv := make([]string, 0, len(jobs))
		for _, job := range jobs {
			if !repo.leasedJobs[job.Id] {
				rv = append(rv, job.Id)
			}
		}
		return rv, nil
	} else {
		return make([]string, 0), nil
	}
}

func (repo *mockJobRepository) GetExistingJobsByIds(jobIds []string) ([]LegacySchedulerJob, error) {
	rv := make([]LegacySchedulerJob, len(jobIds))
	for i, jobId := range jobIds {
		if job, ok := repo.jobsById[jobId]; ok {
			rv[i] = job
		}
	}
	return rv, nil
}

func (repo *mockJobRepository) TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error) {
	successfullyLeasedJobs := make([]*api.Job, 0, len(jobs))
	for _, job := range jobs {
		if !repo.leasedJobs[job.Id] {
			successfullyLeasedJobs = append(successfullyLeasedJobs, job)
			repo.leasedJobs[job.Id] = true
		}
	}
	return successfullyLeasedJobs, nil
}

func benchmarkQuantityComparison(b *testing.B, q1, q2 resource.Quantity) {
	for i := 0; i < b.N; i++ {
		q1.Cmp(q2)
	}
}

func BenchmarkQuantityComparison(b *testing.B) {
	benchmarkQuantityComparison(b, resource.MustParse("1"), resource.MustParse("2"))
}

func BenchmarkIntComparison(b *testing.B) {
	result := 0
	v1 := 1
	v2 := 2
	for i := 0; i < b.N; i++ {
		if v1 == v2 {
			result += 1
		}
	}
}
