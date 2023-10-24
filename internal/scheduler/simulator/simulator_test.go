package simulator

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestSimulator(t *testing.T) {
	tests := map[string]struct {
		clusterSpec            *ClusterSpec
		workloadSpec           *WorkloadSpec
		schedulingConfig       configuration.SchedulingConfig
		expectedEventSequences []*armadaevents.EventSequence
		simulatedTimeLimit     time.Duration
	}{
		"Two jobs in parallel": {
			clusterSpec: &ClusterSpec{
				Name:  "basic",
				Pools: []*Pool{Pool32Cpu("Pool", 1, 1, 2)},
			},
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(
						&Queue{Name: "A", Weight: 1},
						JobTemplate32Cpu(2, "foo", testfixtures.TestDefaultPriorityClass),
					),
				},
			},
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			expectedEventSequences: []*armadaevents.EventSequence{
				{Queue: "A", JobSetName: "foo", Events: testfixtures.Repeat(SubmitJob(), 2)},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
			},
			simulatedTimeLimit: 5 * time.Minute,
		},
		"Two jobs in sequence": {
			clusterSpec: &ClusterSpec{
				Name:  "basic",
				Pools: []*Pool{Pool32Cpu("Pool", 1, 1, 1)},
			},
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(
						&Queue{Name: "A", Weight: 1},
						JobTemplate32Cpu(2, "foo", testfixtures.TestDefaultPriorityClass),
					),
				},
			},
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			expectedEventSequences: []*armadaevents.EventSequence{
				{Queue: "A", JobSetName: "foo", Events: testfixtures.Repeat(SubmitJob(), 2)},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
			},
			simulatedTimeLimit: 5 * time.Minute,
		},
		"10 jobs in sequence": {
			clusterSpec: &ClusterSpec{
				Name:  "basic",
				Pools: []*Pool{Pool32Cpu("Pool", 1, 1, 1)},
			},
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(
						&Queue{Name: "A", Weight: 1},
						JobTemplate32Cpu(10, "foo", testfixtures.TestDefaultPriorityClass),
					),
				},
			},
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			expectedEventSequences: append(
				[]*armadaevents.EventSequence{
					{Queue: "A", JobSetName: "foo", Events: testfixtures.Repeat(SubmitJob(), 10)},
				},
				armadaslices.Repeat(
					10,
					&armadaevents.EventSequence{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
					&armadaevents.EventSequence{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				)...,
			),
			simulatedTimeLimit: 20 * time.Minute,
		},
		"JobTemplate dependencies": {
			clusterSpec: &ClusterSpec{
				Name:  "basic",
				Pools: []*Pool{Pool32Cpu("Pool", 1, 1, 3)},
			},
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(
						&Queue{Name: "A", Weight: 1},
						WithIdJobTemplate(
							JobTemplate32Cpu(2, "foo", testfixtures.TestDefaultPriorityClass),
							"jobTemplate",
						),
						WithDependenciesJobTemplate(
							JobTemplate32Cpu(1, "foo", testfixtures.TestDefaultPriorityClass),
							"jobTemplate",
						),
					),
				},
			},
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			expectedEventSequences: []*armadaevents.EventSequence{
				{Queue: "A", JobSetName: "foo", Events: testfixtures.Repeat(SubmitJob(), 2)},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "A", JobSetName: "foo", Events: testfixtures.Repeat(SubmitJob(), 1)},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
			},
			simulatedTimeLimit: 5 * time.Minute,
		},
		"Preemption": {
			clusterSpec: &ClusterSpec{
				Name:  "basic",
				Pools: []*Pool{Pool32Cpu("Pool", 1, 1, 2)},
			},
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(
						&Queue{Name: "A", Weight: 1},
						JobTemplate32Cpu(2, "foo", testfixtures.PriorityClass0),
					),
					WithJobTemplatesQueue(
						&Queue{Name: "B", Weight: 1},
						WithMinSubmitTimeJobTemplate(
							JobTemplate32Cpu(1, "bar", testfixtures.PriorityClass0),
							30*time.Second,
						),
					),
				},
			},
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			expectedEventSequences: []*armadaevents.EventSequence{
				{Queue: "A", JobSetName: "foo", Events: armadaslices.Repeat(2, SubmitJob())},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "B", JobSetName: "bar", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunPreempted()}},
				{Queue: "B", JobSetName: "bar", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "B", JobSetName: "bar", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
			},
			simulatedTimeLimit: 5 * time.Minute,
		},
		"Preemption cascade": {
			clusterSpec: &ClusterSpec{
				Name: "test",
				Pools: []*Pool{
					WithExecutorGroupsPool(
						&Pool{Name: "Pool"},
						ExecutorGroup32Cpu(1, 1),
						ExecutorGroup32Cpu(1, 1),
						ExecutorGroup32Cpu(1, 1),
					),
				},
			},
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(
						&Queue{Name: "B", Weight: 1},
						JobTemplate32Cpu(1, "foo", testfixtures.PriorityClass0),
					),
					WithJobTemplatesQueue(
						&Queue{Name: "C", Weight: 1},
						JobTemplate32Cpu(2, "foo", testfixtures.PriorityClass0),
					),
					WithJobTemplatesQueue(
						&Queue{Name: "A", Weight: 1},
						WithMinSubmitTimeJobTemplate(
							JobTemplate32Cpu(1, "foo", testfixtures.PriorityClass0),
							30*time.Second,
						),
					),
				},
			},
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			expectedEventSequences: []*armadaevents.EventSequence{
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "C", JobSetName: "foo", Events: armadaslices.Repeat(2, SubmitJob())},
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunPreempted()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunPreempted()}},
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
			},
			simulatedTimeLimit: 5 * time.Minute,
		},
		"No preemption cascade with unified scheduling": {
			clusterSpec: &ClusterSpec{
				Name: "test",
				Pools: []*Pool{
					WithExecutorGroupsPool(
						&Pool{Name: "Pool"},
						ExecutorGroup32Cpu(3, 1),
					),
				},
			},
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(
						&Queue{Name: "B", Weight: 1},
						JobTemplate32Cpu(1, "foo", testfixtures.PriorityClass0),
					),
					WithJobTemplatesQueue(
						&Queue{Name: "C", Weight: 1},
						JobTemplate32Cpu(2, "foo", testfixtures.PriorityClass0),
					),
					WithJobTemplatesQueue(
						&Queue{Name: "A", Weight: 1},
						WithMinSubmitTimeJobTemplate(
							JobTemplate32Cpu(1, "foo", testfixtures.PriorityClass0),
							30*time.Second,
						),
					),
				},
			},
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			expectedEventSequences: []*armadaevents.EventSequence{
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "C", JobSetName: "foo", Events: armadaslices.Repeat(2, SubmitJob())},
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunPreempted()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
			},
			simulatedTimeLimit: 5 * time.Minute,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			simResult, metricsCollector, err := SimulationRun(armadacontext.Background(), tc.clusterSpec, tc.workloadSpec, tc.schedulingConfig)
			require.NoError(t, err)

			t.Logf("Simulation Results: %s", metricsCollector.String())
			if tc.expectedEventSequences != nil {
				require.Equal(
					t,
					util.Map(tc.expectedEventSequences, func(eventSequence *armadaevents.EventSequence) string { return EventSequenceSummary(eventSequence) }),
					util.Map(simResult.Events, func(eventSequence *armadaevents.EventSequence) string { return EventSequenceSummary(eventSequence) }),
					"Expected:\n%s\nReceived:\n%s",
					EventSequencesSummary(tc.expectedEventSequences),
					EventSequencesSummary(simResult.Events),
				)
			}
			require.LessOrEqual(t, metricsCollector.Total.LastJobSuccess, tc.simulatedTimeLimit)
		})
	}
}

func TestSchedulingConfigsFromPattern(t *testing.T) {
	actual, err := SchedulingConfigsFromPattern("./testdata/schedulingConfigs/basicSchedulingConfig.yaml")
	require.NoError(t, err)
	expected := GetBasicSchedulingConfig()
	assert.Equal(t, expected, actual[0])
}

func TestClusterSpecsFromPattern(t *testing.T) {
	clusterSpecs, err := ClusterSpecsFromPattern("./testdata/clusters/tinyCluster.yaml")
	assert.Equal(t, clusterSpecs[0], GetTwoPoolTwoNodeCluster())
	require.NoError(t, err)
}

func TestWorkloadFromPattern(t *testing.T) {
	workloadSpecs, err := WorkloadFromPattern("./testdata/workloads/basicWorkload.yaml")
	assert.Equal(t, workloadSpecs[0], GetOneQueue10JobWorkload())
	require.NoError(t, err)
}

func TestAggregateClusterResourcesEquivalent(t *testing.T) {
	clusterSpecsSegregated, err := ClusterSpecsFromPattern("./testdata/clusters/tinyCluster.yaml")
	require.NoError(t, err)
	clusterSpecsUnified, err := ClusterSpecsFromPattern("./testdata/clusters/tinyClusterAlt.yaml")
	require.NoError(t, err)

	srm := CalculateClusterAggregateResources(clusterSpecsSegregated[0])
	urm := CalculateClusterAggregateResources(clusterSpecsUnified[0])

	require.True(t, reflect.DeepEqual(srm, urm))
	t.Logf("seg, unified | cpu %d %d, gpu %d %d, mem %d %d", srm["cpu"], urm["cpu"], srm["gpu"], urm["gpu"], srm["memory"], urm["memory"])
}
