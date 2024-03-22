package scheduler

import (
	"testing"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/time/rate"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
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
		// Cumulative number of jobs we expect to schedule successfully.
		// Each index `i` is the expected value when processing gang `i`.
		ExpectedCumulativeScheduledJobs []int
		// If present, assert that gang `i` is scheduled on nodes with node
		// uniformity label `ExpectedNodeUniformity[i]`.
		ExpectedNodeUniformity map[int]string
		// The expected number of jobs we successfully scheduled between min gang cardinality and gang cardinality.
		ExpectedRuntimeGangCardinality []int
	}{
		"simple success": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32)),
			},
			ExpectedScheduledIndices:        testfixtures.IntRange(0, 0),
			ExpectedCumulativeScheduledJobs: []int{32},
			ExpectedRuntimeGangCardinality:  []int{32},
		},
		"simple failure": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 33)),
			},
			ExpectedScheduledIndices:        nil,
			ExpectedCumulativeScheduledJobs: []int{0},
			ExpectedRuntimeGangCardinality:  []int{0},
		},
		"simple success where min cardinality is met": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.WithGangAnnotationsAndMinCardinalityJobs(
					32,
					testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 40),
				),
			},
			ExpectedScheduledIndices:        testfixtures.IntRange(0, 0),
			ExpectedCumulativeScheduledJobs: []int{32},
			ExpectedRuntimeGangCardinality:  []int{32},
		},
		"simple failure where min cardinality is not met": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.WithGangAnnotationsAndMinCardinalityJobs(
					33,
					testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 40),
				),
			},
			ExpectedScheduledIndices:        nil,
			ExpectedCumulativeScheduledJobs: []int{0},
			ExpectedRuntimeGangCardinality:  []int{0},
		},
		"one success and one failure": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
			},
			ExpectedScheduledIndices:        testfixtures.IntRange(0, 0),
			ExpectedCumulativeScheduledJobs: []int{32, 32},
			ExpectedRuntimeGangCardinality:  []int{32, 0},
		},
		"one success and one failure using min cardinality": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.WithGangAnnotationsAndMinCardinalityJobs(
					32,
					testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 33),
				),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
			},
			ExpectedScheduledIndices:        testfixtures.IntRange(0, 0),
			ExpectedCumulativeScheduledJobs: []int{32, 32},
			ExpectedRuntimeGangCardinality:  []int{32, 0},
		},
		"multiple nodes": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 64)),
			},
			ExpectedScheduledIndices:        testfixtures.IntRange(0, 0),
			ExpectedCumulativeScheduledJobs: []int{64},
			ExpectedRuntimeGangCardinality:  []int{64},
		},
		"MaximumResourceFractionToSchedule": {
			SchedulingConfig: testfixtures.WithRoundLimitsConfig(
				map[string]float64{"cpu": 0.5},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 8)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 16)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 8)),
			},
			ExpectedScheduledIndices:        []int{0, 1},
			ExpectedCumulativeScheduledJobs: []int{8, 24, 24},
			ExpectedRuntimeGangCardinality:  []int{8, 16, 0},
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
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
			},
			ExpectedScheduledIndices:        []int{0, 1, 2},
			ExpectedCumulativeScheduledJobs: []int{1, 2, 3, 3, 3},
			ExpectedRuntimeGangCardinality:  []int{1, 1, 1, 0, 0},
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
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
			},
			ExpectedScheduledIndices:        []int{0, 1, 2, 3},
			ExpectedCumulativeScheduledJobs: []int{1, 2, 3, 4, 4},
			ExpectedRuntimeGangCardinality:  []int{1, 1, 1, 1, 0},
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
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 2)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 2)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 3)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2, 3)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass2, 4)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass3, 4)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass3, 5)),
			},
			ExpectedScheduledIndices:        []int{0, 2, 4, 6},
			ExpectedCumulativeScheduledJobs: []int{1, 1, 3, 3, 6, 6, 10, 10},
			ExpectedRuntimeGangCardinality:  []int{1, 0, 2, 0, 3, 0, 4, 0},
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
				testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1)),
			},
			ExpectedScheduledIndices:        testfixtures.IntRange(0, 5),
			ExpectedCumulativeScheduledJobs: testfixtures.IntRange(1, 6),
			ExpectedRuntimeGangCardinality:  []int{1, 1, 1, 1, 1, 1},
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
				testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1)),
				testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1)),
			},
			ExpectedScheduledIndices:        testfixtures.IntRange(0, 2),
			ExpectedCumulativeScheduledJobs: []int{1, 2, 3, 3},
			ExpectedRuntimeGangCardinality:  []int{1, 1, 1, 0},
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
				testfixtures.WithGangAnnotationsJobs(testfixtures.N1GpuJobs("A", testfixtures.PriorityClass0, 1)),
			},
			ExpectedScheduledIndices:        testfixtures.IntRange(0, 0),
			ExpectedCumulativeScheduledJobs: []int{1},
			ExpectedRuntimeGangCardinality:  []int{1},
		},
		"NodeUniformityLabel set but not indexed": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes: testfixtures.WithLabelsNodes(
				map[string]string{"foo": "foov"},
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			),
			Gangs: [][]*jobdb.Job{
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.WithNodeUniformityLabelAnnotationJobs(
						"foo",
						testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1),
					)),
			},
			ExpectedScheduledIndices:        nil,
			ExpectedCumulativeScheduledJobs: []int{0},
			ExpectedRuntimeGangCardinality:  []int{0},
		},
		"NodeUniformityLabel not set": {
			SchedulingConfig: testfixtures.WithIndexedNodeLabelsConfig(
				[]string{"foo", "bar"},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Gangs: [][]*jobdb.Job{
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.WithNodeUniformityLabelAnnotationJobs(
						"foo",
						testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1),
					)),
			},
			ExpectedScheduledIndices:        nil,
			ExpectedCumulativeScheduledJobs: []int{0},
			ExpectedRuntimeGangCardinality:  []int{0},
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
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.WithNodeUniformityLabelAnnotationJobs("foo", testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 3)),
				),
			},
			ExpectedScheduledIndices:        nil,
			ExpectedCumulativeScheduledJobs: []int{0},
			ExpectedRuntimeGangCardinality:  []int{0},
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
				testfixtures.WithGangAnnotationsJobs(
					testfixtures.WithNodeUniformityLabelAnnotationJobs(
						"foo",
						testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 4),
					)),
			},
			ExpectedScheduledIndices:        []int{0},
			ExpectedCumulativeScheduledJobs: []int{4},
			ExpectedRuntimeGangCardinality:  []int{4},
		},
		"NodeUniformityLabel NumScheduled tiebreak": {
			SchedulingConfig: testfixtures.WithIndexedNodeLabelsConfig(
				[]string{"my-cool-node-uniformity"},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: append(
				testfixtures.WithLabelsNodes(
					map[string]string{"my-cool-node-uniformity": "a"},
					testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
				),
				testfixtures.WithLabelsNodes(
					map[string]string{"my-cool-node-uniformity": "b"},
					testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
				)...,
			),
			Gangs: [][]*jobdb.Job{
				testfixtures.WithGangAnnotationsAndMinCardinalityJobs(
					2,
					testfixtures.WithNodeUniformityLabelAnnotationJobs(
						"my-cool-node-uniformity",
						testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 4),
					),
				),
			},
			ExpectedScheduledIndices:        []int{0},
			ExpectedCumulativeScheduledJobs: []int{3},
			ExpectedNodeUniformity:          map[int]string{0: "b"},
			ExpectedRuntimeGangCardinality:  []int{3},
		},
		"NodeUniformityLabel PreemptedAtPriority tiebreak": {
			SchedulingConfig: testfixtures.WithIndexedNodeLabelsConfig(
				[]string{"my-cool-node-uniformity"},
				testfixtures.TestSchedulingConfig(),
			),
			Nodes: append(
				testfixtures.WithUsedResourcesNodes(
					1,
					schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
					testfixtures.WithLabelsNodes(
						map[string]string{"my-cool-node-uniformity": "a"},
						testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
					),
				),
				testfixtures.WithUsedResourcesNodes(
					0,
					schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
					testfixtures.WithLabelsNodes(
						map[string]string{"my-cool-node-uniformity": "b"},
						testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
					),
				)...,
			),
			Gangs: [][]*jobdb.Job{
				testfixtures.WithGangAnnotationsAndMinCardinalityJobs(
					2,
					testfixtures.WithNodeUniformityLabelAnnotationJobs(
						"my-cool-node-uniformity",
						testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass2, 4),
					),
				),
			},
			ExpectedScheduledIndices:        []int{0},
			ExpectedCumulativeScheduledJobs: []int{2},
			ExpectedNodeUniformity:          map[int]string{0: "b"},
			ExpectedRuntimeGangCardinality:  []int{2},
		},
		"AwayNodeTypes": {
			SchedulingConfig: func() configuration.SchedulingConfig {
				config := testfixtures.TestSchedulingConfig()
				config.PriorityClasses = map[string]types.PriorityClass{
					"armada-preemptible-away": {
						Priority:    30000,
						Preemptible: true,

						AwayNodeTypes: []types.AwayNodeType{
							{Priority: 29000, WellKnownNodeTypeName: "node-type-a"},
							{Priority: 29000, WellKnownNodeTypeName: "node-type-b"},
						},
					},
					"armada-preemptible-away-both": {
						Priority:    30000,
						Preemptible: true,

						AwayNodeTypes: []types.AwayNodeType{
							{Priority: 29000, WellKnownNodeTypeName: "node-type-ab"},
						},
					},
				}
				config.DefaultPriorityClassName = "armada-preemptible-away"
				config.WellKnownNodeTypes = []configuration.WellKnownNodeType{
					{
						Name: "node-type-a",
						Taints: []v1.Taint{
							{Key: "taint-a", Value: "true", Effect: v1.TaintEffectNoSchedule},
						},
					},
					{
						Name: "node-type-b",
						Taints: []v1.Taint{
							{Key: "taint-b", Value: "true", Effect: v1.TaintEffectNoSchedule},
						},
					},
					{
						Name: "node-type-ab",
						Taints: []v1.Taint{
							{Key: "taint-a", Value: "true", Effect: v1.TaintEffectNoSchedule},
							{Key: "taint-b", Value: "true", Effect: v1.TaintEffectNoSchedule},
						},
					},
				}
				return config
			}(),
			Nodes: func() []*schedulerobjects.Node {
				nodes := testfixtures.N8GpuNodes(1, []int32{29000, 30000})
				for _, node := range nodes {
					node.Taints = []v1.Taint{
						{Key: "taint-a", Value: "true", Effect: v1.TaintEffectNoSchedule},
						{Key: "taint-b", Value: "true", Effect: v1.TaintEffectNoSchedule},
					}
				}
				return nodes
			}(),
			Gangs: func() (gangs [][]*jobdb.Job) {
				var jobId ulid.ULID
				jobId = util.ULID()
				gangs = append(gangs, []*jobdb.Job{testfixtures.TestJob("A", jobId, "armada-preemptible-away", testfixtures.Test1Cpu4GiPodReqs("A", jobId, 30000))})
				jobId = util.ULID()
				gangs = append(gangs, []*jobdb.Job{testfixtures.TestJob("A", jobId, "armada-preemptible-away-both", testfixtures.Test1Cpu4GiPodReqs("A", jobId, 30000))})
				return
			}(),
			ExpectedScheduledIndices:        []int{1},
			ExpectedCumulativeScheduledJobs: []int{0, 1},
			ExpectedRuntimeGangCardinality:  []int{0, 1},
		},
		"Home-away scheduling": {
			SchedulingConfig: func() configuration.SchedulingConfig {
				config := testfixtures.TestSchedulingConfig()
				config.PriorityClasses = map[string]types.PriorityClass{
					"armada-preemptible": {
						Priority:    30000,
						Preemptible: true,
					},
					"armada-preemptible-away": {
						Priority:    30000,
						Preemptible: true,
						AwayNodeTypes: []types.AwayNodeType{
							{Priority: 29000, WellKnownNodeTypeName: "node-type-a"},
						},
					},
				}
				config.DefaultPriorityClassName = "armada-preemptible"
				config.WellKnownNodeTypes = []configuration.WellKnownNodeType{
					{
						Name: "node-type-a",
						Taints: []v1.Taint{
							{Key: "taint-a", Value: "true", Effect: v1.TaintEffectNoSchedule},
						},
					},
				}
				return config
			}(),
			Nodes: func() []*schedulerobjects.Node {
				nodes := testfixtures.N32CpuNodes(1, []int32{29000, 30000})
				for _, node := range nodes {
					node.Taints = []v1.Taint{
						{Key: "taint-a", Value: "true", Effect: v1.TaintEffectNoSchedule},
					}
				}
				return nodes
			}(),
			Gangs: func() (gangs [][]*jobdb.Job) {
				jobId := util.ULID()
				gangs = append(gangs, []*jobdb.Job{testfixtures.TestJob("A", jobId, "armada-preemptible-away", testfixtures.Test32Cpu256GiPodReqs("A", jobId, 30000))})
				jobId = util.ULID()
				gangs = append(gangs, []*jobdb.Job{testfixtures.TestJob("A", jobId, "armada-preemptible-away", testfixtures.Test32Cpu256GiPodReqs("A", jobId, 30000))})
				return
			}(),
			ExpectedScheduledIndices:        []int{0},
			ExpectedCumulativeScheduledJobs: []int{1, 1},
			ExpectedRuntimeGangCardinality:  []int{1, 0},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodesById := make(map[string]*schedulerobjects.Node, len(tc.Nodes))
			for _, node := range tc.Nodes {
				nodesById[node.Id] = node
			}
			nodeDb, err := nodedb.NewNodeDb(
				tc.SchedulingConfig.PriorityClasses,
				tc.SchedulingConfig.MaxExtraNodesToConsider,
				tc.SchedulingConfig.IndexedResources,
				tc.SchedulingConfig.IndexedTaints,
				tc.SchedulingConfig.IndexedNodeLabels,
				tc.SchedulingConfig.WellKnownNodeTypes,
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
				tc.SchedulingConfig.PriorityClasses,
				tc.SchedulingConfig.DefaultPriorityClassName,
				fairnessCostProvider,
				rate.NewLimiter(
					rate.Limit(tc.SchedulingConfig.MaximumSchedulingRate),
					tc.SchedulingConfig.MaximumSchedulingBurst,
				),
				tc.TotalResources,
			)
			for queue, priorityFactor := range priorityFactorByQueue {
				err := sctx.AddQueueSchedulingContext(
					queue,
					priorityFactor,
					nil,
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
				nil,
			)
			sch, err := NewGangScheduler(sctx, constraints, nodeDb)
			require.NoError(t, err)

			var actualScheduledIndices []int
			scheduledGangs := 0
			for i, gang := range tc.Gangs {
				jctxs := schedulercontext.JobSchedulingContextsFromJobs(tc.SchedulingConfig.PriorityClasses, gang)
				gctx := schedulercontext.NewGangSchedulingContext(jctxs)
				ok, reason, err := sch.Schedule(armadacontext.Background(), gctx)
				require.NoError(t, err)

				// Runtime cardinality should match expected scheduled jobs
				require.Equal(t, tc.ExpectedRuntimeGangCardinality[i], gctx.Fit().NumScheduled)

				if ok {
					require.Empty(t, reason)
					actualScheduledIndices = append(actualScheduledIndices, i)

					// If there's a node uniformity constraint, check that it's met.
					if nodeUniformity := gctx.GangInfo.NodeUniformity; nodeUniformity != "" {
						nodeUniformityLabelValues := make(map[string]bool)
						for _, jctx := range jctxs {
							pctx := jctx.PodSchedulingContext
							if !pctx.IsSuccessful() {
								continue
							}
							node := nodesById[pctx.NodeId]
							require.NotNil(t, node)
							value, ok := node.Labels[nodeUniformity]
							require.True(t, ok, "gang job scheduled onto node with missing nodeUniformityLabel")
							nodeUniformityLabelValues[value] = true
						}
						require.Equal(
							t, 1, len(nodeUniformityLabelValues),
							"node uniformity constraint not met: %s", nodeUniformityLabelValues,
						)
						if expectedValue, ok := tc.ExpectedNodeUniformity[i]; ok {
							actualValue := maps.Keys(nodeUniformityLabelValues)[0]
							require.Equal(t, expectedValue, actualValue)
						}
					}

					// Verify any excess jobs that failed have the correct state set
					for _, jctx := range jctxs {
						if jctx.ShouldFail {
							if jctx.PodSchedulingContext != nil {
								require.Equal(t, "", jctx.PodSchedulingContext.NodeId)
							}
							require.Equal(t, "job does not fit on any node", jctx.UnschedulableReason)
						}
					}

					// Verify accounting
					scheduledGangs++
					require.Equal(t, scheduledGangs, sch.schedulingContext.NumScheduledGangs)
					require.Equal(t, tc.ExpectedCumulativeScheduledJobs[i], sch.schedulingContext.NumScheduledJobs)
					require.Equal(t, 0, sch.schedulingContext.NumEvictedJobs)
				} else {
					require.NotEmpty(t, reason)

					// Verify all jobs have been correctly unbound from nodes
					for _, jctx := range jctxs {
						if jctx.PodSchedulingContext != nil {
							require.Equal(t, "", jctx.PodSchedulingContext.NodeId)
						}
					}

					// Verify accounting
					require.Equal(t, scheduledGangs, sch.schedulingContext.NumScheduledGangs)
					require.Equal(t, tc.ExpectedCumulativeScheduledJobs[i], sch.schedulingContext.NumScheduledJobs)
					require.Equal(t, 0, sch.schedulingContext.NumEvictedJobs)
				}
			}
			assert.Equal(t, tc.ExpectedScheduledIndices, actualScheduledIndices)
		})
	}
}
