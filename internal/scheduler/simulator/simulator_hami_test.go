package simulator

import (
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/hami"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/simulator/sink"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// hamiPlacement is where a run was placed: its node and the HAMi GPUs reserved for it.
type hamiPlacement struct {
	queue     string
	jobSet    string
	nodeId    string
	devices   []string
	memoryMiB []int64
}

func hamiSchedulingConfig() configuration.SchedulingConfig {
	config := testfixtures.TestSchedulingConfigWithPools([]configuration.PoolConfig{
		{Name: testfixtures.TestPool, Hami: configuration.HamiPoolConfig{Enabled: true}},
	})
	config.SupportedResourceTypes = append(config.SupportedResourceTypes,
		configuration.ResourceType{Name: hami.GPUMemoryResource, Resolution: resource.MustParse("1")},
		configuration.ResourceType{Name: hami.GPUCoreResource, Resolution: resource.MustParse("1")},
	)
	config.DominantResourceFairnessResourcesToConsider = append(slices.Clone(config.DominantResourceFairnessResourcesToConsider),
		hami.GPUMemoryResource, hami.GPUCoreResource)
	return config
}

func TestSimulatorHami(t *testing.T) {
	enableFastForward := false
	schedulerCyclePeriodSeconds := 10
	// One node with two 16 GiB HAMi GPUs of 4 slots each.
	hamiCluster := &ClusterSpec{
		Name: "hami",
		Clusters: []*Cluster{
			{
				Name:          "TestCluster",
				Pool:          testfixtures.TestPool,
				NodeTemplates: []*NodeTemplate{NodeTemplateHamiGpu(1, 2, 16384)},
			},
		},
	}
	quarterGpu := func(n int64, jobSet, priorityClass string) *JobTemplate {
		return JobTemplateHamiGpu(n, jobSet, priorityClass, 1, 4096, 25)
	}
	wholeGpu := func(n int64, jobSet, priorityClass string) *JobTemplate {
		return JobTemplateHamiGpu(n, jobSet, priorityClass, 1, 0, 0)
	}
	tests := map[string]struct {
		clusterSpec            *ClusterSpec
		workloadSpec           *WorkloadSpec
		schedulingConfig       configuration.SchedulingConfig
		expectedEventSequences []*armadaevents.EventSequence
		// Checked against the placements of the leased runs, in order of leasing,
		// and of the preempted runs.
		checkPlacements    func(t *testing.T, leased, preempted []hamiPlacement)
		simulatedTimeLimit time.Duration
	}{
		"Fractional jobs bin pack": {
			clusterSpec: hamiCluster,
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(&Queue{Name: "A", Weight: 1}, quarterGpu(6, "foo", testfixtures.TestDefaultPriorityClass)),
				},
			},
			schedulingConfig: hamiSchedulingConfig(),
			expectedEventSequences: armadaslices.Concatenate(
				armadaslices.Repeat(1, SubmitJob(6, "A", "foo")),
				armadaslices.Repeat(6, JobRunLeased(1, "A", "foo")),
				armadaslices.Repeat(6, JobSucceeded(1, "A", "foo")),
			),
			checkPlacements: func(t *testing.T, leased, _ []hamiPlacement) {
				assert.Equal(t, []int{2, 4}, jobsPerDevice(leased), "6 quarter jobs fill one GPU and put the rest on the other")
				for _, p := range leased {
					assert.Equal(t, []int64{4096}, p.memoryMiB)
				}
			},
			simulatedTimeLimit: 5 * time.Minute,
		},
		"Fragmented devices block a job that fits in total": {
			clusterSpec: hamiCluster,
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(
						&Queue{Name: "A", Weight: 1},
						// Two 5/8-device fillers take one GPU each. The half-device job
						// then fits in the node's total free memory, but on no single GPU.
						JobTemplateHamiGpu(2, "filler", testfixtures.TestDefaultPriorityClass, 1, 10240, 62),
						WithMinSubmitTimeJobTemplate(JobTemplateHamiGpu(1, "half", testfixtures.TestDefaultPriorityClass, 1, 8192, 50), 5*time.Second),
					),
				},
			},
			schedulingConfig: hamiSchedulingConfig(),
			expectedEventSequences: armadaslices.Concatenate(
				armadaslices.Repeat(1, SubmitJob(2, "A", "filler")),
				armadaslices.Repeat(2, JobRunLeased(1, "A", "filler")),
				armadaslices.Repeat(1, SubmitJob(1, "A", "half")),
				armadaslices.Repeat(2, JobSucceeded(1, "A", "filler")),
				armadaslices.Repeat(1, JobRunLeased(1, "A", "half")),
				armadaslices.Repeat(1, JobSucceeded(1, "A", "half")),
			),
			checkPlacements: func(t *testing.T, leased, _ []hamiPlacement) {
				require.Len(t, leased, 3)
				assert.NotEqual(t, leased[0].devices, leased[1].devices, "a 5/8 filler cannot share a GPU")
				assert.Contains(t, [][]string{leased[0].devices, leased[1].devices}, leased[2].devices, "the half job takes a freed GPU")
			},
			simulatedTimeLimit: 5 * time.Minute,
		},
		"Multi-GPU job uses distinct devices": {
			clusterSpec: hamiCluster,
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(&Queue{Name: "A", Weight: 1}, JobTemplateHamiGpu(1, "foo", testfixtures.TestDefaultPriorityClass, 2, 4096, 25)),
				},
			},
			schedulingConfig: hamiSchedulingConfig(),
			expectedEventSequences: []*armadaevents.EventSequence{
				SubmitJob(1, "A", "foo"),
				JobRunLeased(1, "A", "foo"),
				JobSucceeded(1, "A", "foo"),
			},
			checkPlacements: func(t *testing.T, leased, _ []hamiPlacement) {
				require.Len(t, leased, 1)
				assert.ElementsMatch(t, []string{"gpu-0", "gpu-1"}, leased[0].devices)
				assert.Equal(t, []int64{4096, 4096}, leased[0].memoryMiB, "memory is per GPU")
			},
			simulatedTimeLimit: 5 * time.Minute,
		},
		"Whole-device job is exclusive": {
			clusterSpec: hamiCluster,
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(
						&Queue{Name: "A", Weight: 1},
						wholeGpu(1, "whole", testfixtures.TestDefaultPriorityClass),
						quarterGpu(5, "quarter", testfixtures.TestDefaultPriorityClass),
					),
				},
			},
			schedulingConfig: hamiSchedulingConfig(),
			expectedEventSequences: armadaslices.Concatenate(
				armadaslices.Repeat(1, SubmitJob(1, "A", "whole")),
				armadaslices.Repeat(1, SubmitJob(5, "A", "quarter")),
				armadaslices.Repeat(1, JobRunLeased(1, "A", "whole")),
				armadaslices.Repeat(4, JobRunLeased(1, "A", "quarter")),
				armadaslices.Repeat(1, JobSucceeded(1, "A", "whole")),
				armadaslices.Repeat(4, JobSucceeded(1, "A", "quarter")),
				armadaslices.Repeat(1, JobRunLeased(1, "A", "quarter")),
				armadaslices.Repeat(1, JobSucceeded(1, "A", "quarter")),
			),
			checkPlacements: func(t *testing.T, leased, _ []hamiPlacement) {
				require.Len(t, leased, 6)
				whole := leased[0]
				assert.Equal(t, "whole", whole.jobSet)
				assert.Equal(t, []int64{16384}, whole.memoryMiB, "a plain GPU request reserves the whole device")
				for _, p := range leased[1:5] {
					assert.NotEqual(t, whole.devices, p.devices, "fractional jobs avoid the whole-device job's GPU")
				}
			},
			simulatedTimeLimit: 5 * time.Minute,
		},
		"GPU jobs stay on HAMi nodes": {
			clusterSpec: &ClusterSpec{
				Name: "hami-and-plain",
				Clusters: []*Cluster{
					{
						Name: "TestCluster",
						Pool: testfixtures.TestPool,
						// A HAMi node with two GPUs and a plain node with eight GPUs, not registered with HAMi.
						NodeTemplates: []*NodeTemplate{NodeTemplateHamiGpu(1, 2, 16384), NodeTemplateGpu(1)},
					},
				},
			},
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(&Queue{Name: "A", Weight: 1}, wholeGpu(3, "foo", testfixtures.TestDefaultPriorityClass)),
				},
			},
			schedulingConfig: hamiSchedulingConfig(),
			expectedEventSequences: armadaslices.Concatenate(
				armadaslices.Repeat(1, SubmitJob(3, "A", "foo")),
				armadaslices.Repeat(2, JobRunLeased(1, "A", "foo")),
				armadaslices.Repeat(2, JobSucceeded(1, "A", "foo")),
				armadaslices.Repeat(1, JobRunLeased(1, "A", "foo")),
				armadaslices.Repeat(1, JobSucceeded(1, "A", "foo")),
			),
			checkPlacements: func(t *testing.T, leased, _ []hamiPlacement) {
				for _, p := range leased {
					assert.Equal(t, "TestCluster-0-0", p.nodeId, "the plain GPU node receives no GPU jobs")
					assert.Len(t, p.devices, 1)
				}
			},
			simulatedTimeLimit: 5 * time.Minute,
		},
		"Queues converge to fair share": {
			clusterSpec: hamiCluster,
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(&Queue{Name: "A", Weight: 1}, quarterGpu(8, "foo", testfixtures.TestDefaultPriorityClass)),
					WithJobTemplatesQueue(&Queue{Name: "B", Weight: 1}, quarterGpu(8, "bar", testfixtures.TestDefaultPriorityClass)),
				},
			},
			schedulingConfig: hamiSchedulingConfig(),
			expectedEventSequences: armadaslices.Concatenate(
				armadaslices.Repeat(1, SubmitJob(8, "A", "foo")),
				armadaslices.Repeat(1, SubmitJob(8, "B", "bar")),
				armadaslices.Repeat(4, JobRunLeased(1, "A", "foo")),
				armadaslices.Repeat(4, JobRunLeased(1, "B", "bar")),
				armadaslices.Repeat(4, JobSucceeded(1, "A", "foo")),
				armadaslices.Repeat(4, JobSucceeded(1, "B", "bar")),
				armadaslices.Repeat(4, JobRunLeased(1, "A", "foo")),
				armadaslices.Repeat(4, JobRunLeased(1, "B", "bar")),
				armadaslices.Repeat(4, JobSucceeded(1, "A", "foo")),
				armadaslices.Repeat(4, JobSucceeded(1, "B", "bar")),
			),
			simulatedTimeLimit: 5 * time.Minute,
		},
		"Fair-share preemption across queues": {
			clusterSpec: hamiCluster,
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(&Queue{Name: "A", Weight: 1}, quarterGpu(8, "foo", testfixtures.PriorityClass0)),
					WithJobTemplatesQueue(
						&Queue{Name: "B", Weight: 1},
						WithMinSubmitTimeJobTemplate(quarterGpu(4, "bar", testfixtures.PriorityClass0), 30*time.Second),
					),
				},
			},
			schedulingConfig: hamiSchedulingConfig(),
			expectedEventSequences: armadaslices.Concatenate(
				armadaslices.Repeat(1, SubmitJob(8, "A", "foo")),
				armadaslices.Repeat(8, JobRunLeased(1, "A", "foo")),
				armadaslices.Repeat(1, SubmitJob(4, "B", "bar")),
				// Queue B takes its fair share: exactly four of A's jobs are preempted and resubmitted.
				armadaslices.Repeat(4, JobRunPreempted(1, "A", "foo")),
				armadaslices.Repeat(4, JobRunLeased(1, "B", "bar")),
				armadaslices.Repeat(4, SubmitJob(1, "A", "foo")),
				armadaslices.Repeat(4, JobSucceeded(1, "A", "foo")),
				armadaslices.Repeat(4, JobRunLeased(1, "A", "foo")),
				armadaslices.Repeat(4, JobSucceeded(1, "B", "bar")),
				armadaslices.Repeat(4, JobSucceeded(1, "A", "foo")),
			),
			checkPlacements: func(t *testing.T, leased, preempted []hamiPlacement) {
				for _, p := range leased {
					assert.Len(t, p.devices, 1, "every lease, including retries, has a GPU")
				}
				assert.Len(t, preempted, 4)
			},
			simulatedTimeLimit: 5 * time.Minute,
		},
		"Gang is all or nothing": {
			clusterSpec: hamiCluster,
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(
						&Queue{Name: "A", Weight: 1},
						wholeGpu(1, "filler", testfixtures.TestDefaultPriorityClass),
						// The two-GPU gang waits while only one GPU is free.
						WithMinSubmitTimeJobTemplate(hamiGang(wholeGpu(2, "gang", testfixtures.TestDefaultPriorityClass), 2), 5*time.Second),
					),
				},
			},
			schedulingConfig: hamiSchedulingConfig(),
			expectedEventSequences: armadaslices.Concatenate(
				armadaslices.Repeat(1, SubmitJob(1, "A", "filler")),
				armadaslices.Repeat(1, JobRunLeased(1, "A", "filler")),
				armadaslices.Repeat(1, SubmitJob(2, "A", "gang")),
				armadaslices.Repeat(1, JobSucceeded(1, "A", "filler")),
				armadaslices.Repeat(2, JobRunLeased(1, "A", "gang")),
				armadaslices.Repeat(2, JobSucceeded(1, "A", "gang")),
			),
			checkPlacements: func(t *testing.T, leased, _ []hamiPlacement) {
				require.Len(t, leased, 3)
				assert.NotEqual(t, leased[1].devices, leased[2].devices, "the gang's jobs get distinct GPUs")
			},
			simulatedTimeLimit: 5 * time.Minute,
		},
		"Urgency preemption": {
			clusterSpec: hamiCluster,
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(
						&Queue{Name: "A", Weight: 1},
						quarterGpu(8, "low", testfixtures.PriorityClass0),
						// A higher-priority whole-GPU job takes one GPU from the four jobs holding it.
						WithMinSubmitTimeJobTemplate(wholeGpu(1, "urgent", testfixtures.PriorityClass3), 5*time.Second),
					),
				},
			},
			schedulingConfig: hamiSchedulingConfig(),
			expectedEventSequences: armadaslices.Concatenate(
				armadaslices.Repeat(1, SubmitJob(8, "A", "low")),
				armadaslices.Repeat(8, JobRunLeased(1, "A", "low")),
				armadaslices.Repeat(1, SubmitJob(1, "A", "urgent")),
				armadaslices.Repeat(4, JobRunPreempted(1, "A", "low")),
				armadaslices.Repeat(1, JobRunLeased(1, "A", "urgent")),
				armadaslices.Repeat(4, SubmitJob(1, "A", "low")),
				armadaslices.Repeat(4, JobSucceeded(1, "A", "low")),
				armadaslices.Repeat(4, JobRunLeased(1, "A", "low")),
				armadaslices.Repeat(1, JobSucceeded(1, "A", "urgent")),
				armadaslices.Repeat(4, JobSucceeded(1, "A", "low")),
			),
			checkPlacements: func(t *testing.T, leased, preempted []hamiPlacement) {
				require.Len(t, leased, 13)
				urgent := leased[8]
				require.Equal(t, "urgent", urgent.jobSet)
				require.Len(t, preempted, 4)
				for _, p := range preempted {
					assert.Equal(t, urgent.devices, p.devices, "exactly the jobs holding the urgent job's GPU are preempted")
				}
			},
			simulatedTimeLimit: 5 * time.Minute,
		},
		"Unhealthy devices are avoided": {
			clusterSpec: &ClusterSpec{
				Name: "unhealthy",
				Clusters: []*Cluster{
					{
						Name:          "TestCluster",
						Pool:          testfixtures.TestPool,
						NodeTemplates: []*NodeTemplate{NodeTemplateHamiGpu(1, 2, 16384, 0)},
					},
				},
			},
			workloadSpec: &WorkloadSpec{
				Queues: []*Queue{
					WithJobTemplatesQueue(&Queue{Name: "A", Weight: 1}, wholeGpu(2, "foo", testfixtures.TestDefaultPriorityClass)),
				},
			},
			schedulingConfig: hamiSchedulingConfig(),
			expectedEventSequences: armadaslices.Concatenate(
				armadaslices.Repeat(1, SubmitJob(2, "A", "foo")),
				armadaslices.Repeat(1, JobRunLeased(1, "A", "foo")),
				armadaslices.Repeat(1, JobSucceeded(1, "A", "foo")),
				armadaslices.Repeat(1, JobRunLeased(1, "A", "foo")),
				armadaslices.Repeat(1, JobSucceeded(1, "A", "foo")),
			),
			checkPlacements: func(t *testing.T, leased, _ []hamiPlacement) {
				for _, p := range leased {
					assert.Equal(t, []string{"gpu-1"}, p.devices, "the unhealthy GPU gets no work")
				}
			},
			simulatedTimeLimit: 5 * time.Minute,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s, err := NewSimulator(tc.clusterSpec, tc.workloadSpec, tc.schedulingConfig, enableFastForward, int((tc.simulatedTimeLimit + time.Hour).Minutes()), schedulerCyclePeriodSeconds, sink.NullSink{})
			require.NoError(t, err)
			start := s.time
			actualEventSequences := make([]*armadaevents.EventSequence, 0, 128)
			var leased, preempted []hamiPlacement
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
						for i, event := range stateTransition.EventSequence.Events {
							switch event.Event.(type) {
							case *armadaevents.EventSequence_Event_JobRunLeased:
								leased = append(leased, placementOf(stateTransition.Jobs[i]))
							case *armadaevents.EventSequence_Event_JobRunPreempted:
								preempted = append(preempted, placementOf(stateTransition.Jobs[i]))
							}
						}
					}
				}
			})
			g.Go(func() error {
				return s.Run(ctx)
			})
			err = g.Wait()
			require.NoError(t, err)
			require.Equal(
				t,
				armadaslices.Map(tc.expectedEventSequences, func(eventSequence *armadaevents.EventSequence) string { return EventSequenceSummary(eventSequence) }),
				armadaslices.Map(actualEventSequences, func(eventSequence *armadaevents.EventSequence) string { return EventSequenceSummary(eventSequence) }),
				"Expected:\n%s\nReceived:\n%s",
				EventSequencesSummary(tc.expectedEventSequences),
				EventSequencesSummary(actualEventSequences),
			)
			if tc.checkPlacements != nil {
				tc.checkPlacements(t, leased, preempted)
			}
			require.LessOrEqual(t, s.time.Sub(start), tc.simulatedTimeLimit)
		})
	}
}

func hamiGang(jobTemplate *JobTemplate, cardinality uint32) *JobTemplate {
	jobTemplate.GangCardinality = cardinality
	return jobTemplate
}

func placementOf(job *jobdb.Job) hamiPlacement {
	run := job.LatestRun()
	p := hamiPlacement{queue: job.Queue(), jobSet: job.Jobset(), nodeId: run.NodeId()}
	for _, allocation := range run.HamiDeviceAllocations() {
		p.devices = append(p.devices, allocation.Id)
		p.memoryMiB = append(p.memoryMiB, allocation.MemoryMib)
	}
	return p
}

// jobsPerDevice returns how many placements use each GPU, sorted ascending.
func jobsPerDevice(placements []hamiPlacement) []int {
	countByDevice := map[string]int{}
	for _, p := range placements {
		for _, device := range p.devices {
			countByDevice[p.nodeId+"/"+device]++
		}
	}
	counts := make([]int, 0, len(countByDevice))
	for _, count := range countByDevice {
		counts = append(counts, count)
	}
	sort.Ints(counts)
	return counts
}
