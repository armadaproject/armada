package simulator

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/simulator/sink"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestSimulator(t *testing.T) {
	enableFastForward := false
	schedulerCyclePeriodSeconds := 10
	tests := map[string]struct {
		clusterSpec            *ClusterSpec
		workloadSpec           *WorkloadSpec
		schedulingConfig       configuration.SchedulingConfig
		expectedEventSequences []*armadaevents.EventSequence
		simulatedTimeLimit     time.Duration
	}{
		"Two jobs in parallel": {
			clusterSpec: &ClusterSpec{
				Name: "basic",
				Clusters: []*Cluster{
					{
						Name:          "TestCluster",
						Pool:          "TestPool",
						NodeTemplates: []*NodeTemplate{NodeTemplate32Cpu(2)},
					},
				},
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
				Name: "basic",
				Clusters: []*Cluster{
					{
						Name:          "TestCluster",
						Pool:          "TestPool",
						NodeTemplates: []*NodeTemplate{NodeTemplate32Cpu(1)},
					},
				},
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
				Name: "basic",
				Clusters: []*Cluster{
					{
						Name:          "TestCluster",
						Pool:          "TestPool",
						NodeTemplates: []*NodeTemplate{NodeTemplate32Cpu(1)},
					},
				},
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
		"Multiple Clusters": {
			clusterSpec: &ClusterSpec{
				Name: "basic",
				Clusters: []*Cluster{
					{
						Name:          "ClusterA",
						Pool:          "TestPool",
						NodeTemplates: []*NodeTemplate{NodeTemplate32Cpu(1)},
					},
					{
						Name:          "ClusterB",
						Pool:          "TestPool",
						NodeTemplates: []*NodeTemplate{NodeTemplate32Cpu(1)},
					},
				},
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
		"JobTemplate dependencies": {
			clusterSpec: &ClusterSpec{
				Name: "basic",
				Clusters: []*Cluster{
					{
						Name:          "TestCluster",
						Pool:          "TestPool",
						NodeTemplates: []*NodeTemplate{NodeTemplate32Cpu(3)},
					},
				},
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
				Name: "basic",
				Clusters: []*Cluster{
					{
						Name:          "TestCluster",
						Pool:          "TestPool",
						NodeTemplates: []*NodeTemplate{NodeTemplate32Cpu(2)},
					},
				},
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
		"No preemption cascade": {
			clusterSpec: &ClusterSpec{
				Name: "test",
				Clusters: []*Cluster{
					{
						Name:          "TestCluster1",
						Pool:          "TestPool",
						NodeTemplates: []*NodeTemplate{NodeTemplate32Cpu(1)},
					},
					{
						Name:          "TestCluster2",
						Pool:          "TestPool",
						NodeTemplates: []*NodeTemplate{NodeTemplate32Cpu(1)},
					},
					{
						Name:          "TestCluster3",
						Pool:          "TestPool",
						NodeTemplates: []*NodeTemplate{NodeTemplate32Cpu(1)},
					},
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
				Clusters: []*Cluster{
					{
						Name:          "TestCluster",
						Pool:          "TestPool",
						NodeTemplates: []*NodeTemplate{NodeTemplate32Cpu(2)},
					},
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
				Clusters: func() []*Cluster {
					whaleNodeTemplate := NodeTemplateGpu(2)
					whaleNodeTemplate.Taints = []*v1.Taint{
						{Key: "gpu-whale", Value: "true", Effect: v1.TaintEffectNoSchedule},
					}
					return []*Cluster{
						{
							Name:          "WhaleCluster",
							Pool:          "TestPool",
							NodeTemplates: []*NodeTemplate{whaleNodeTemplate},
						},
						{
							Name:          "TestCluster",
							Pool:          "TestPool",
							NodeTemplates: []*NodeTemplate{NodeTemplateGpu(2)},
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
								Number:            2,
								JobSet:            "job-set-0",
								PriorityClassName: "armada-preemptible",
								Requirements: &schedulerobjects.PodRequirements{
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
								EarliestSubmitTime:  protoutil.ToDuration(1 * time.Minute),
								RuntimeDistribution: &ShiftedExponential{Minimum: protoutil.ToDuration(5 * time.Minute)},
							},
						},
					},
					{
						Name:   "queue-1",
						Weight: 1,
						JobTemplates: []*JobTemplate{
							{
								Id:                "queue-1-template-0",
								Number:            32,
								JobSet:            "job-set-1",
								PriorityClassName: "armada-preemptible-away",
								Requirements: &schedulerobjects.PodRequirements{
									ResourceRequirements: v1.ResourceRequirements{
										Requests: v1.ResourceList{
											"cpu":            resource.MustParse("16"),
											"memory":         resource.MustParse("512Gi"),
											"nvidia.com/gpu": resource.MustParse("1"),
										},
									},
								},
								RuntimeDistribution: &ShiftedExponential{Minimum: protoutil.ToDuration(time.Hour)},
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
				// Submit 32 1-gpu jobs to fill up all nodes.  16 Jobs will be scheduled away
				armadaslices.Repeat(1, SubmitJob(32, "queue-1", "job-set-1")),
				armadaslices.Repeat(32, JobRunLeased(1, "queue-1", "job-set-1")),

				// Submit 2 whole node-whale jobs
				armadaslices.Repeat(1, SubmitJob(2, "queue-0", "job-set-0")),

				// 16 Jobs should be preempted to make way and 2 of the whale jobs should start run
				armadaslices.Repeat(16, JobRunPreempted(1, "queue-1", "job-set-1")),
				armadaslices.Repeat(2, JobRunLeased(1, "queue-0", "job-set-0")),

				// The 16 preempted jobs should be resubmitted
				armadaslices.Repeat(16, SubmitJob(1, "queue-1", "job-set-1")),

				// The 2 whale jobs finish, which means that 16 more 1-gpu jbs can be scheduled
				armadaslices.Repeat(2, JobSucceeded(1, "queue-0", "job-set-0")),
				armadaslices.Repeat(16, JobRunLeased(1, "queue-1", "job-set-1")),

				// finally all the 1-gpu jobs finish
				armadaslices.Repeat(32, JobSucceeded(1, "queue-1", "job-set-1")),
			),
			simulatedTimeLimit: 24 * time.Hour,
		},
		"Gang Job": {
			clusterSpec: &ClusterSpec{
				Name: "basic",
				Clusters: []*Cluster{
					{
						Name:          "Cluster1",
						Pool:          "TestPool",
						NodeTemplates: []*NodeTemplate{NodeTemplate32Cpu(8)},
					},
					{
						// This cluster should be too small to run gangs
						Name:          "Cluster2",
						Pool:          "TestPool",
						NodeTemplates: []*NodeTemplate{NodeTemplate32Cpu(1)},
					},
				},
			},
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(
						&Queue{Name: "A", Weight: 1},
						GangJobTemplate32Cpu(16, 8, "foo", testfixtures.TestDefaultPriorityClass),
					),
				},
			},
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			expectedEventSequences: armadaslices.Concatenate(
				armadaslices.Repeat(1, SubmitJob(16, "A", "foo")),
				armadaslices.Repeat(8, JobRunLeased(1, "A", "foo")),
				armadaslices.Repeat(8, JobSucceeded(1, "A", "foo")),
				armadaslices.Repeat(8, JobRunLeased(1, "A", "foo")),
				armadaslices.Repeat(8, JobSucceeded(1, "A", "foo")),
			),
			simulatedTimeLimit: 5 * time.Minute,
		},
		"Preempted Gang Job": {
			clusterSpec: &ClusterSpec{
				Name: "basic",
				Clusters: []*Cluster{
					{
						Name:          "Cluster1",
						Pool:          "TestPool",
						NodeTemplates: []*NodeTemplate{NodeTemplate32Cpu(8)},
					},
				},
			},
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(
						&Queue{Name: "A", Weight: 1},
						GangJobTemplate32Cpu(8, 8, "foo", testfixtures.PriorityClass2),
					),
					WithJobTemplatesQueue(
						&Queue{Name: "B", Weight: 1},
						WithMinSubmitTimeJobTemplate(
							JobTemplate32Cpu(1, "bar", testfixtures.PriorityClass3),
							30*time.Second,
						),
					),
				},
			},
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			expectedEventSequences: armadaslices.Concatenate(
				armadaslices.Repeat(1, SubmitJob(8, "A", "foo")),
				armadaslices.Repeat(8, JobRunLeased(1, "A", "foo")),
				armadaslices.Repeat(1, SubmitJob(1, "B", "bar")),
				armadaslices.Repeat(8, JobRunPreempted(1, "A", "foo")),
				armadaslices.Repeat(1, JobRunLeased(1, "B", "bar")),
				armadaslices.Repeat(8, SubmitJob(1, "A", "foo")),
				armadaslices.Repeat(1, JobSucceeded(1, "B", "bar")),
				armadaslices.Repeat(8, JobRunLeased(1, "A", "foo")),
				armadaslices.Repeat(8, JobSucceeded(1, "A", "foo")),
			),
			simulatedTimeLimit: 5 * time.Minute,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s, err := NewSimulator(tc.clusterSpec, tc.workloadSpec, tc.schedulingConfig, enableFastForward, int((tc.simulatedTimeLimit + time.Hour).Minutes()), schedulerCyclePeriodSeconds, sink.NullSink{})
			require.NoError(t, err)
			start := s.time
			actualEventSequences := make([]*armadaevents.EventSequence, 0, 128)
			c := s.StateTransitions()

			ctx := armadacontext.Background()
			g, ctx := armadacontext.ErrGroup(ctx)
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
			if tc.expectedEventSequences != nil {
				require.Equal(
					t,
					armadaslices.Map(tc.expectedEventSequences, func(eventSequence *armadaevents.EventSequence) string { return EventSequenceSummary(eventSequence) }),
					armadaslices.Map(actualEventSequences, func(eventSequence *armadaevents.EventSequence) string { return EventSequenceSummary(eventSequence) }),
					"Expected:\n%s\nReceived:\n%s",
					EventSequencesSummary(tc.expectedEventSequences),
					EventSequencesSummary(actualEventSequences),
				)
			}
			require.LessOrEqual(t, s.time.Sub(start), tc.simulatedTimeLimit)
		})
	}
}

func TestGenerateRandomShiftedExponentialDuration(t *testing.T) {
	assert.Equal(
		t,
		time.Hour,
		generateRandomShiftedExponentialDuration(
			rand.New(rand.NewSource(0)),
			&ShiftedExponential{
				Minimum: protoutil.ToDuration(time.Hour),
			},
		),
	)
	assert.Less(
		t,
		time.Hour,
		generateRandomShiftedExponentialDuration(
			rand.New(rand.NewSource(0)),
			&ShiftedExponential{
				Minimum:  protoutil.ToDuration(time.Hour),
				TailMean: protoutil.ToDuration(time.Second),
			},
		),
	)
}
