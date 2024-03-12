package simulator

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	schedulerobjects "github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
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
				SubmitJob(2, "A", "foo"),
				JobRunLeased(1, "A", "foo"),
				JobRunLeased(1, "A", "foo"),
				JobSucceeded(1, "A", "foo"),
				JobSucceeded(1, "A", "foo"),
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
				SubmitJob(2, "A", "foo"),
				JobRunLeased(1, "A", "foo"),
				JobSucceeded(1, "A", "foo"),
				JobRunLeased(1, "A", "foo"),
				JobSucceeded(1, "A", "foo"),
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
			expectedEventSequences: armadaslices.Concatenate(
				armadaslices.Repeat(1, SubmitJob(10, "A", "foo")),
				armadaslices.Repeat(10, JobRunLeased(1, "A", "foo"), JobSucceeded(1, "A", "foo")),
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
				SubmitJob(2, "A", "foo"),
				JobRunLeased(1, "A", "foo"),
				JobRunLeased(1, "A", "foo"),
				JobSucceeded(1, "A", "foo"),
				JobSucceeded(1, "A", "foo"),
				SubmitJob(1, "A", "foo"),
				JobRunLeased(1, "A", "foo"),
				JobSucceeded(1, "A", "foo"),
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
				SubmitJob(2, "A", "foo"),
				JobRunLeased(1, "A", "foo"),
				JobRunLeased(1, "A", "foo"),
				SubmitJob(1, "B", "bar"),
				JobRunPreempted(1, "A", "foo"),
				JobRunLeased(1, "B", "bar"),
				SubmitJob(1, "A", "foo"),
				JobSucceeded(1, "A", "foo"),
				JobRunLeased(1, "A", "foo"),
				JobSucceeded(1, "B", "bar"),
				JobSucceeded(1, "A", "foo"),
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
				SubmitJob(1, "B", "foo"),
				SubmitJob(2, "C", "foo"),
				JobRunLeased(1, "B", "foo"),
				JobRunLeased(1, "C", "foo"),
				JobRunLeased(1, "C", "foo"),
				SubmitJob(1, "A", "foo"),
				JobRunPreempted(1, "B", "foo"),
				JobRunLeased(1, "A", "foo"),
				SubmitJob(1, "B", "foo"),
				JobRunPreempted(1, "C", "foo"),
				JobRunLeased(1, "B", "foo"),
				SubmitJob(1, "C", "foo"),
				JobSucceeded(1, "C", "foo"),
				JobRunLeased(1, "C", "foo"),
				JobSucceeded(1, "A", "foo"),
				JobSucceeded(1, "B", "foo"),
				JobSucceeded(1, "C", "foo"),
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
				SubmitJob(1, "B", "foo"),
				SubmitJob(2, "C", "foo"),
				JobRunLeased(1, "B", "foo"),
				JobRunLeased(1, "C", "foo"),
				JobRunLeased(1, "C", "foo"),
				SubmitJob(1, "A", "foo"),
				JobRunPreempted(1, "C", "foo"),
				JobRunLeased(1, "A", "foo"),
				SubmitJob(1, "C", "foo"),
				JobSucceeded(1, "B", "foo"),
				JobSucceeded(1, "C", "foo"),
				JobRunLeased(1, "C", "foo"),
				JobSucceeded(1, "A", "foo"),
				JobSucceeded(1, "C", "foo"),
			},
			simulatedTimeLimit: 5 * time.Minute,
		},
		"Consistent job ordering": {
			clusterSpec: &ClusterSpec{
				Name: "test",
				Pools: []*Pool{
					WithExecutorGroupsPool(
						&Pool{Name: "Pool"},
						ExecutorGroup32Cpu(1, 2),
					),
				},
			},
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(
						&Queue{Name: "A", Weight: 1},
						JobTemplate1Cpu(64, "foo", testfixtures.PriorityClass0),
					),
					WithJobTemplatesQueue(
						&Queue{Name: "B", Weight: 1},
						WithMinSubmitTimeJobTemplate(
							JobTemplate32Cpu(2, "foo", testfixtures.PriorityClass0),
							30*time.Minute,
						),
					),
				},
			},
			schedulingConfig:       testfixtures.TestSchedulingConfig(),
			expectedEventSequences: nil,
			simulatedTimeLimit:     2*time.Hour + 30*time.Minute,
		},
		"Home-away preemption": {
			clusterSpec: &ClusterSpec{
				Name: "cluster",
				Pools: func() []*Pool {
					whaleNodeTemplate := NodeTemplateGpu(2)
					whaleNodeTemplate.Taints = []v1.Taint{
						{Key: "gpu-whale", Value: "true", Effect: v1.TaintEffectNoSchedule},
					}
					whaleCluster := Cluster{NodeTemplates: []*NodeTemplate{whaleNodeTemplate}}
					return []*Pool{
						{
							Name: "pool",
							ClusterGroups: []*ClusterGroup{
								{
									Clusters: []*Cluster{
										{NodeTemplates: []*NodeTemplate{NodeTemplateGpu(2)}},
										&whaleCluster,
									},
								},
							},
						},
					}
				}(),
			},
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					{
						Name:   "queue-0",
						Weight: 1,
						JobTemplates: []*JobTemplate{
							{
								Id:                "queue-0-template-0",
								Number:            1,
								JobSet:            "job-set-0",
								PriorityClassName: "armada-preemptible",
								Requirements: schedulerobjects.PodRequirements{
									ResourceRequirements: v1.ResourceRequirements{
										Requests: v1.ResourceList{
											"cpu":            resource.MustParse("128"),
											"memory":         resource.MustParse("4096Gi"),
											"nvidia.com/gpu": resource.MustParse("8"),
										},
									},
									Tolerations: []v1.Toleration{
										{Key: "gpu-whale", Value: "true", Effect: v1.TaintEffectNoSchedule},
									},
								},
								RuntimeDistribution: ShiftedExponential{Minimum: 5 * time.Minute},
							},
							{
								Id:                "queue-0-template-1",
								Number:            4,
								JobSet:            "job-set-0",
								PriorityClassName: "armada-preemptible",
								Requirements: schedulerobjects.PodRequirements{
									ResourceRequirements: v1.ResourceRequirements{
										Requests: v1.ResourceList{
											"cpu":            resource.MustParse("128"),
											"memory":         resource.MustParse("4096Gi"),
											"nvidia.com/gpu": resource.MustParse("8"),
										},
									},
									Tolerations: []v1.Toleration{
										{Key: "gpu-whale", Value: "true", Effect: v1.TaintEffectNoSchedule},
									},
								},
								Dependencies:        []string{"queue-0-template-0"},
								RuntimeDistribution: ShiftedExponential{Minimum: time.Hour},
							},
						},
					},
					{
						Name:   "queue-1",
						Weight: 1,
						JobTemplates: []*JobTemplate{
							{
								Number:            32,
								JobSet:            "job-set-1",
								PriorityClassName: "armada-preemptible-away",
								Requirements: schedulerobjects.PodRequirements{
									ResourceRequirements: v1.ResourceRequirements{
										Requests: v1.ResourceList{
											"cpu":            resource.MustParse("16"),
											"memory":         resource.MustParse("512Gi"),
											"nvidia.com/gpu": resource.MustParse("1"),
										},
									},
								},
								RuntimeDistribution: ShiftedExponential{Minimum: time.Hour},
							},
						},
					},
				},
			},
			schedulingConfig: func() configuration.SchedulingConfig {
				config := testfixtures.TestSchedulingConfig()
				config.PriorityClasses = map[string]types.PriorityClass{
					"armada-preemptible-away": {
						Priority:    30000,
						Preemptible: true,

						AwayNodeTypes: []types.AwayNodeType{{Priority: 29000, WellKnownNodeTypeName: "gpu-whale"}},
					},
					"armada-preemptible": {
						Priority:    30000,
						Preemptible: true,
					},
				}
				config.DefaultPriorityClassName = "armada-preemptible"
				config.WellKnownNodeTypes = []configuration.WellKnownNodeType{
					{
						Name:   "gpu-whale",
						Taints: []v1.Taint{{Key: "gpu-whale", Value: "true", Effect: v1.TaintEffectNoSchedule}},
					},
				}
				return config
			}(),
			expectedEventSequences: armadaslices.Concatenate(
				armadaslices.Repeat(1, SubmitJob(1, "queue-0", "job-set-0")),
				armadaslices.Repeat(1, SubmitJob(32, "queue-1", "job-set-1")),
				armadaslices.Repeat(1, JobRunLeased(1, "queue-0", "job-set-0")),
				armadaslices.Repeat(24, JobRunLeased(1, "queue-1", "job-set-1")),
				armadaslices.Repeat(1, JobSucceeded(1, "queue-0", "job-set-0")),
				armadaslices.Repeat(1, SubmitJob(4, "queue-0", "job-set-0")),
				armadaslices.Repeat(8, JobRunLeased(1, "queue-1", "job-set-1")),
				armadaslices.Repeat(24, JobRunPreempted(1, "queue-1", "job-set-1")),
				armadaslices.Repeat(3, JobRunLeased(1, "queue-0", "job-set-0")),
				armadaslices.Repeat(24, SubmitJob(1, "queue-1", "job-set-1")),
				armadaslices.Repeat(8, JobSucceeded(1, "queue-1", "job-set-1")),
				armadaslices.Repeat(8, JobRunLeased(1, "queue-1", "job-set-1")),
				armadaslices.Repeat(3, JobSucceeded(1, "queue-0", "job-set-0")),
				armadaslices.Repeat(1, JobRunLeased(1, "queue-0", "job-set-0")),
				armadaslices.Repeat(16, JobRunLeased(1, "queue-1", "job-set-1")),
				armadaslices.Repeat(8, JobSucceeded(1, "queue-1", "job-set-1")),
				armadaslices.Repeat(1, JobSucceeded(1, "queue-0", "job-set-0")),
				armadaslices.Repeat(16, JobSucceeded(1, "queue-1", "job-set-1")),
			),
			simulatedTimeLimit: 24 * time.Hour,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s, err := NewSimulator(tc.clusterSpec, tc.workloadSpec, tc.schedulingConfig)
			require.NoError(t, err)
			mc := NewMetricsCollector(s.StateTransitions())
			actualEventSequences := make([]*armadaevents.EventSequence, 0, 128)
			c := s.StateTransitions()

			ctx := armadacontext.Background()
			g, ctx := armadacontext.ErrGroup(ctx)
			g.Go(func() error {
				return mc.Run(ctx)
			})
			g.Go(func() error {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case stateTransition, ok := <-c:
						if !ok {
							return nil
						}
						t.Log(*stateTransition.EventSequence.Events[0].Created, EventSequenceSummary(stateTransition.EventSequence))
						actualEventSequences = append(actualEventSequences, stateTransition.EventSequence)
					}
				}
			})
			g.Go(func() error {
				return s.Run(ctx)
			})
			err = g.Wait()
			require.NoError(t, err)

			t.Logf("Simulation Results: %s", mc.String())
			if tc.expectedEventSequences != nil {
				require.Equal(
					t,
					util.Map(tc.expectedEventSequences, func(eventSequence *armadaevents.EventSequence) string { return EventSequenceSummary(eventSequence) }),
					util.Map(actualEventSequences, func(eventSequence *armadaevents.EventSequence) string { return EventSequenceSummary(eventSequence) }),
					"Expected:\n%s\nReceived:\n%s",
					EventSequencesSummary(tc.expectedEventSequences),
					EventSequencesSummary(actualEventSequences),
				)
			}
			require.LessOrEqual(t, mc.OverallMetrics.TimeOfMostRecentJobSucceededEvent, tc.simulatedTimeLimit)
		})
	}
}

func TestSchedulingConfigsFromPattern(t *testing.T) {
	actual, err := SchedulingConfigsFromPattern("./testdata/configs/basicSchedulingConfig.yaml")
	require.NoError(t, err)
	expected := []configuration.SchedulingConfig{GetBasicSchedulingConfig()}
	assert.Equal(t, expected, actual)
}

func TestClusterSpecsFromPattern(t *testing.T) {
	clusterSpecs, err := ClusterSpecsFromPattern("./testdata/clusters/tinyCluster.yaml")
	require.NoError(t, err)
	assert.Equal(t, []*ClusterSpec{GetTwoPoolTwoNodeCluster()}, clusterSpecs)
	require.NoError(t, err)
}

func TestWorkloadsFromPattern(t *testing.T) {
	workloadSpecs, err := WorkloadsFromPattern("./testdata/workloads/basicWorkload.yaml")
	require.NoError(t, err)
	assert.Equal(t, []*WorkloadSpec{GetOneQueue10JobWorkload()}, workloadSpecs)
	require.NoError(t, err)
}

func TestClusterSpecTotalResources(t *testing.T) {
	actual := GetTwoPoolTwoNodeCluster().TotalResources()
	expected := schedulerobjects.ResourceList{
		Resources: map[string]resource.Quantity{
			"cpu":            resource.MustParse("160"),
			"memory":         resource.MustParse("4352Gi"),
			"nvidia.com/gpu": resource.MustParse("8"),
		},
	}
	assert.True(t, expected.Equal(actual), "expected %s, but got %s", expected.CompactString(), actual.CompactString())
}

func TestGenerateRandomShiftedExponentialDuration(t *testing.T) {
	assert.Equal(
		t,
		time.Hour,
		generateRandomShiftedExponentialDuration(
			rand.New(rand.NewSource(0)),
			ShiftedExponential{
				Minimum: time.Hour,
			},
		),
	)
	assert.Less(
		t,
		time.Hour,
		generateRandomShiftedExponentialDuration(
			rand.New(rand.NewSource(0)),
			ShiftedExponential{
				Minimum:  time.Hour,
				TailMean: time.Second,
			},
		),
	)
}
