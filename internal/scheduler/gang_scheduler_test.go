package scheduler

import (
	"github.com/armadaproject/armada/internal/common/context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/fairness"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
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
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
			},
			ExpectedScheduledIndices: testfixtures.IntRange(0, 0),
		},
		"simple failure": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 33),
			},
			ExpectedScheduledIndices: nil,
		},
		"one success and one failure": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
			},
			ExpectedScheduledIndices: testfixtures.IntRange(0, 0),
		},
		"multiple nodes": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 64),
			},
			ExpectedScheduledIndices: testfixtures.IntRange(0, 0),
		},
		"MaximumResourceFractionToSchedule": {
			SchedulingConfig: testfixtures.WithRoundLimitsConfig(
				map[string]float64{"cpu": 0.5},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 8),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 16),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 8),
			},
			ExpectedScheduledIndices: []int{0, 1},
		},
		"MaximumResourceFractionToScheduleByPool": {
			SchedulingConfig: testfixtures.WithRoundLimitsConfig(
				map[string]float64{"cpu": 0.5},
				testfixtures.WithRoundLimitsPoolConfig(
					map[string]map[string]float64{"pool": {"cpu": 2.0 / 32.0}},
					testfixtures.TestSchedulingConfig(),
				),
			),
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
			},
			ExpectedScheduledIndices: []int{0, 1, 2},
		},
		"MaximumResourceFractionToScheduleByPool non-existing pool": {
			SchedulingConfig: testfixtures.WithRoundLimitsConfig(
				map[string]float64{"cpu": 3.0 / 32.0},
				testfixtures.WithRoundLimitsPoolConfig(
					map[string]map[string]float64{"this does not exist": {"cpu": 2.0 / 32.0}},
					testfixtures.TestSchedulingConfig(),
				),
			),
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
			},
			ExpectedScheduledIndices: []int{0, 1, 2, 3},
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
			Gangs: [][]*jobdb.Job{
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 2),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 2),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 3),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2, 3),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2, 4),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass3, 4),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass3, 5),
			},
			ExpectedScheduledIndices: []int{0, 2, 4, 6},
		},
		"resolution has no impact on jobs of size a multiple of the resolution": {
			SchedulingConfig: testfixtures.WithIndexedResourcesConfig(
				[]configuration.IndexedResource{
					{Name: "cpu", Resolution: resource.MustParse("16")},
					{Name: "memory", Resolution: resource.MustParse("128Mi")},
				},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1),
			},
			ExpectedScheduledIndices: testfixtures.IntRange(0, 5),
		},
		"jobs of size not a multiple of the resolution blocks scheduling new jobs": {
			SchedulingConfig: testfixtures.WithIndexedResourcesConfig(
				[]configuration.IndexedResource{
					{Name: "cpu", Resolution: resource.MustParse("17")},
					{Name: "memory", Resolution: resource.MustParse("128Mi")},
				},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1),
				testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1),
			},
			ExpectedScheduledIndices: testfixtures.IntRange(0, 2),
		},
		"consider all nodes in the bucket": {
			SchedulingConfig: testfixtures.WithIndexedResourcesConfig(
				[]configuration.IndexedResource{
					{Name: "cpu", Resolution: resource.MustParse("1")},
					{Name: "memory", Resolution: resource.MustParse("1Mi")},
					{Name: "gpu", Resolution: resource.MustParse("1")},
				},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: armadaslices.Concatenate(
				testfixtures.WithUsedResourcesNodes(
					0,
					schedulerobjects.ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("31.5"),
							"memory": resource.MustParse("512Gi"),
							"gpu":    resource.MustParse("8"),
						},
					},
					testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
				),
				testfixtures.WithUsedResourcesNodes(
					0,
					schedulerobjects.ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("32"),
						},
					},
					testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
				),
			),
			Gangs: [][]*jobdb.Job{
				testfixtures.N1GpuJobs("A", testfixtures.PriorityClass0, 1),
			},
			ExpectedScheduledIndices: testfixtures.IntRange(0, 0),
		},
		"NodeUniformityLabel set but not indexed": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: testfixtures.WithLabelsNodes(
				map[string]string{"foo": "foov"},
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			),
			Gangs: [][]*jobdb.Job{
				testfixtures.WithNodeUniformityLabelAnnotationJobs(
					"foo",
					testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1),
				),
			},
			ExpectedScheduledIndices: nil,
		},
		"NodeUniformityLabel not set": {
			SchedulingConfig: testfixtures.WithIndexedNodeLabelsConfig(
				[]string{"foo", "bar"},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.WithNodeUniformityLabelAnnotationJobs(
					"foo",
					testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1),
				),
			},
			ExpectedScheduledIndices: nil,
		},
		"NodeUniformityLabel insufficient capacity": {
			SchedulingConfig: testfixtures.WithIndexedNodeLabelsConfig(
				[]string{"foo", "bar"},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: armadaslices.Concatenate(
				testfixtures.WithLabelsNodes(
					map[string]string{"foo": "foov1"},
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				),
				testfixtures.WithLabelsNodes(
					map[string]string{"foo": "foov2"},
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				),
			),
			Gangs: [][]*jobdb.Job{
				testfixtures.WithNodeUniformityLabelAnnotationJobs(
					"foo",
					testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 3),
				),
			},
			ExpectedScheduledIndices: nil,
		},
		"NodeUniformityLabel": {
			SchedulingConfig: testfixtures.WithIndexedNodeLabelsConfig(
				[]string{"foo", "bar"},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: armadaslices.Concatenate(
				testfixtures.WithLabelsNodes(
					map[string]string{"foo": "foov1"},
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
						testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
					),
				),
				testfixtures.WithLabelsNodes(
					map[string]string{"foo": "foov2"},
					testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
				),
				testfixtures.WithLabelsNodes(
					map[string]string{"foo": "foov3"},
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
						testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
					),
				),
			),
			Gangs: [][]*jobdb.Job{
				testfixtures.WithNodeUniformityLabelAnnotationJobs(
					"foo",
					testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 4),
				),
			},
			ExpectedScheduledIndices: []int{0},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodesById := make(map[string]*schedulerobjects.Node, len(tc.Nodes))
			for _, node := range tc.Nodes {
				nodesById[node.Id] = node
			}
			nodeDb, err := nodedb.NewNodeDb(
				testfixtures.TestPriorityClasses,
				testfixtures.TestMaxExtraNodesToConsider,
				tc.SchedulingConfig.IndexedResources,
				testfixtures.TestIndexedTaints,
				tc.SchedulingConfig.IndexedNodeLabels,
			)
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
			priorityFactorByQueue := make(map[string]float64)
			for _, jobs := range tc.Gangs {
				for _, job := range jobs {
					priorityFactorByQueue[job.GetQueue()] = 1
				}
			}

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
			for queue, priorityFactor := range priorityFactorByQueue {
				err := sctx.AddQueueSchedulingContext(queue, priorityFactor, nil)
				require.NoError(t, err)
			}
			constraints := schedulerconstraints.SchedulingConstraintsFromSchedulingConfig(
				"pool",
				tc.TotalResources,
				schedulerobjects.ResourceList{Resources: tc.MinimumJobSize},
				tc.SchedulingConfig,
			)
			sch, err := NewGangScheduler(sctx, constraints, nodeDb)
			require.NoError(t, err)

			var actualScheduledIndices []int
			for i, gang := range tc.Gangs {
				jctxs := schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, gang)
				gctx := schedulercontext.NewGangSchedulingContext(jctxs)
				ok, reason, err := sch.Schedule(context.Background(), gctx)
				require.NoError(t, err)
				if ok {
					require.Empty(t, reason)
					actualScheduledIndices = append(actualScheduledIndices, i)

					// If there's a node uniformity constraint, check that it's met.
					if gctx.NodeUniformityLabel != "" {
						nodeUniformityLabelValues := make(map[string]bool)
						for _, jctx := range jctxs {
							require.NotNil(t, jctx.PodSchedulingContext)
							node := nodesById[jctx.PodSchedulingContext.NodeId]
							require.NotNil(t, node)
							value, ok := node.Labels[gctx.NodeUniformityLabel]
							require.True(t, ok, "gang job scheduled onto node with missing nodeUniformityLabel")
							nodeUniformityLabelValues[value] = true
						}
						require.Equal(
							t, 1, len(nodeUniformityLabelValues),
							"node uniformity constraint not met: %s", nodeUniformityLabelValues,
						)
					}
				} else {
					require.NotEmpty(t, reason)
				}
			}
			assert.Equal(t, tc.ExpectedScheduledIndices, actualScheduledIndices)
		})
	}
}
