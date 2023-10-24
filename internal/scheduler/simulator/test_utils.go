package simulator

import (
	"fmt"
	"math"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func SimulationRun(ctx *armadacontext.Context, clusterSpec *ClusterSpec, workloadSpec *WorkloadSpec, schedulingConfig configuration.SchedulingConfig) (*SimulationResult, *MetricsCollector, error) {
	s, err := NewSimulator(clusterSpec, workloadSpec, schedulingConfig)
	if err != nil {
		return &SimulationResult{}, &MetricsCollector{}, err
	}

	metricsCollector := NewMetricsCollector(s.Output())
	actualEventSequences := make([]*armadaevents.EventSequence, 0, 128)
	c := s.Output()

	g, threadCtx := armadacontext.ErrGroup(ctx)
	g.Go(func() error {
		return metricsCollector.Run(threadCtx)
	})
	g.Go(func() error {
		for {
			select {
			case <-threadCtx.Done():
				return threadCtx.Err()
			case eventSequence, ok := <-c:
				if !ok {
					return nil
				}
				ctx.Info(*eventSequence.Events[0].Created, EventSequenceSummary(eventSequence))
				actualEventSequences = append(actualEventSequences, eventSequence)
			}
		}
	})
	g.Go(func() error {
		return s.Run(threadCtx)
	})
	if err := g.Wait(); err != nil {
		return &SimulationResult{}, metricsCollector, err
	}

	simResult := &SimulationResult{
		Events:       actualEventSequences,
		ClusterSpec:  clusterSpec,
		WorkloadSpec: workloadSpec,
	}

	return simResult, metricsCollector, nil
}

func GetTwoPoolTwoNodeCluster() *ClusterSpec {
	cs := &ClusterSpec{
		Name: "Tiny Cluster",
		Pools: []*Pool{
			Pool32Cpu("pool1", 1, 1, 1),
			PoolGpu("pool2", 1, 1, 1),
		},
	}
	initialiseClusterSpec(cs)
	return cs
}

func GetOneQueue10JobWorkload() *WorkloadSpec {
	ws := &WorkloadSpec{
		Name: "Basic Workload",
		Queues: []*Queue{
			WithJobTemplatesQueue(
				&Queue{Name: "A", Weight: 1},
				JobTemplate1Cpu(10, "", "armada-default", "myFirstJobTemplate"),
			),
		},
	}
	initialiseWorkloadSpec(ws)
	return ws
}

func GetBasicSchedulingConfig() configuration.SchedulingConfig {
	return configuration.SchedulingConfig{
		Preemption: configuration.PreemptionConfig{
			NodeEvictionProbability: 1.0,
			PriorityClasses: map[string]types.PriorityClass{
				"armada-default": {
					Priority:    30000,
					Preemptible: false,
				},
				"armada-preemptible": {
					Priority:    30000,
					Preemptible: true,
				},
			},
		},
		MaximumResourceFractionToSchedule: map[string]float64{
			"memory": 0.025,
			"cpu":    0.025,
		},
		FairnessModel: "DominantResourceFairness",
		DominantResourceFairnessResourcesToConsider: []string{"cpu", "memory", "nvidia.com/gpu", "ephemeral-storage"},
		IndexedResources: []configuration.IndexedResource{
			{
				Name:       "cpu",
				Resolution: resource.MustParse("1"),
			},
			{
				Name:       "memory",
				Resolution: resource.MustParse("1Mi"),
			},
			{
				Name:       "nvidia.com/gpu",
				Resolution: resource.MustParse("1"),
			},
		},
		MaximumSchedulingRate:          math.Inf(1),
		MaximumSchedulingBurst:         math.MaxInt,
		MaximumPerQueueSchedulingRate:  math.Inf(1),
		MaximumPerQueueSchedulingBurst: math.MaxInt,
	}
}

// Calculates the aggregate number/amount of CPU cores, GPUs and Memory (GB) within a cluster.
func CalculateClusterAggregateResources(clusterSpec *ClusterSpec) map[string]int64 {
	resourceMapping := map[string]int64{
		"cpus":   0,
		"gpus":   0,
		"memory": 0,
	}

	for _, pool := range clusterSpec.Pools {
		for _, clusterGroup := range pool.ClusterGroups {
			for _, cluster := range clusterGroup.Clusters {
				for _, nt := range cluster.NodeTemplates {
					count := nt.Number
					thisNodeCores := nt.TotalResources.Resources["cpu"]
					resourceMapping["cpus"] += thisNodeCores.Value() * count
					thisNodeGPUs := nt.TotalResources.Resources["nvidia.com/gpu"]
					resourceMapping["gpus"] += thisNodeGPUs.Value() * count
					thisNodeMemory := nt.TotalResources.Resources["memory"]
					resourceMapping["memory"] += thisNodeMemory.Value() * count
				}
			}
		}
	}

	return resourceMapping
}

func WithExecutorGroupsPool(pool *Pool, executorGroups ...*ClusterGroup) *Pool {
	pool.ClusterGroups = append(pool.ClusterGroups, executorGroups...)
	return pool
}

func WithExecutorsExecutorGroup(executorGroup *ClusterGroup, executors ...*Cluster) *ClusterGroup {
	executorGroup.Clusters = append(executorGroup.Clusters, executors...)
	return executorGroup
}

func WithNodeTemplatesExecutor(executor *Cluster, nodeTemplates ...*NodeTemplate) *Cluster {
	executor.NodeTemplates = append(executor.NodeTemplates, nodeTemplates...)
	return executor
}

func Pool32Cpu(name string, numExecutorGroups, numExecutorsPerGroup, numNodesPerExecutor int64) *Pool {
	executorGroups := make([]*ClusterGroup, numExecutorGroups)
	for i := 0; i < int(numExecutorGroups); i++ {
		executorGroups[i] = ExecutorGroup32Cpu(numExecutorsPerGroup, numNodesPerExecutor)
	}
	return &Pool{
		Name:          name,
		ClusterGroups: executorGroups,
	}
}

func PoolGpu(name string, numExecutorGroups, numExecutorsPerGroup, numNodesPerExecutor int64) *Pool {
	executorGroups := make([]*ClusterGroup, numExecutorGroups)
	for i := 0; i < int(numExecutorGroups); i++ {
		executorGroups[i] = ExecutorGroupGpu(numExecutorsPerGroup, numNodesPerExecutor)
	}
	return &Pool{
		Name:          name,
		ClusterGroups: executorGroups,
	}
}

func ExecutorGroup32Cpu(numExecutors, numNodesPerExecutor int64) *ClusterGroup {
	executors := make([]*Cluster, numExecutors)
	for i := 0; i < int(numExecutors); i++ {
		executors[i] = Executor32Cpu(numNodesPerExecutor)
	}
	return &ClusterGroup{
		Clusters: executors,
	}
}

func ExecutorGroupGpu(numExecutors, numNodesPerExecutor int64) *ClusterGroup {
	executors := make([]*Cluster, numExecutors)
	for i := 0; i < int(numExecutors); i++ {
		executors[i] = ExecutorGpu(numNodesPerExecutor)
	}
	return &ClusterGroup{
		Clusters: executors,
	}
}

func Executor32Cpu(numNodes int64) *Cluster {
	return &Cluster{
		NodeTemplates: []*NodeTemplate{
			NodeTemplate32Cpu(numNodes),
		},
	}
}

func ExecutorGpu(numNodes int64) *Cluster {
	return &Cluster{
		NodeTemplates: []*NodeTemplate{
			NodeTemplateGpu(numNodes),
		},
	}
}

func NodeTemplate32Cpu(n int64) *NodeTemplate {
	return &NodeTemplate{
		Number: n,
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("256Gi"),
			},
		},
	}
}

func NodeTemplateGpu(n int64) *NodeTemplate {
	return &NodeTemplate{
		Number: n,
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":            resource.MustParse("128"),
				"memory":         resource.MustParse("4096Gi"),
				"nvidia.com/gpu": resource.MustParse("8"),
			},
		},
	}
}

func WithJobTemplatesQueue(queue *Queue, jobTemplate ...*JobTemplate) *Queue {
	queue.JobTemplates = append(queue.JobTemplates, jobTemplate...)
	return queue
}

func WithIdJobTemplate(jobTemplate *JobTemplate, id string) *JobTemplate {
	jobTemplate.Id = id
	return jobTemplate
}

func WithDependenciesJobTemplate(jobTemplate *JobTemplate, dependencyIds ...string) *JobTemplate {
	jobTemplate.Dependencies = append(jobTemplate.Dependencies, dependencyIds...)
	return jobTemplate
}

func WithMinSubmitTimeJobTemplate(jobTemplate *JobTemplate, minSubmitTime time.Duration) *JobTemplate {
	jobTemplate.EarliestSubmitTime = minSubmitTime
	return jobTemplate
}

func JobTemplate32Cpu(n int64, jobSet, priorityClassName string) *JobTemplate {
	return &JobTemplate{
		Number:            n,
		JobSet:            jobSet,
		PriorityClassName: priorityClassName,
		Requirements: schedulerobjects.PodRequirements{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    resource.MustParse("32"),
					"memory": resource.MustParse("256Gi"),
				},
			},
		},
		RuntimeMin: 60 * time.Second,
	}
}

func JobTemplate1Cpu(n int64, jobSet, priorityClassName string, id string) *JobTemplate {
	return &JobTemplate{
		Number:            n,
		JobSet:            jobSet,
		Id:                id,
		PriorityClassName: priorityClassName,
		Requirements: schedulerobjects.PodRequirements{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    resource.MustParse("1"),
					"memory": resource.MustParse("10Gi"),
				},
			},
		},

		RuntimeMin:      10 * time.Minute,
		RuntimeVariance: 1 * time.Minute,
	}
}

func SubmitJob() *armadaevents.EventSequence_Event {
	return &armadaevents.EventSequence_Event{
		Event: &armadaevents.EventSequence_Event_SubmitJob{
			SubmitJob: &armadaevents.SubmitJob{},
		},
	}
}

func JobRunLeased() *armadaevents.EventSequence_Event {
	return &armadaevents.EventSequence_Event{
		Event: &armadaevents.EventSequence_Event_JobRunLeased{
			JobRunLeased: &armadaevents.JobRunLeased{},
		},
	}
}

func JobRunPreempted() *armadaevents.EventSequence_Event {
	return &armadaevents.EventSequence_Event{
		Event: &armadaevents.EventSequence_Event_JobRunPreempted{
			JobRunPreempted: &armadaevents.JobRunPreempted{},
		},
	}
}

func JobSucceeded() *armadaevents.EventSequence_Event {
	return &armadaevents.EventSequence_Event{
		Event: &armadaevents.EventSequence_Event_JobSucceeded{
			JobSucceeded: &armadaevents.JobSucceeded{},
		},
	}
}

func EventSequencesSummary(eventSequences []*armadaevents.EventSequence) string {
	var sb strings.Builder
	for i, eventSequence := range eventSequences {
		sb.WriteString(EventSequenceSummary(eventSequence))
		if i != len(eventSequences)-1 {
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

func EventSequenceSummary(eventSequence *armadaevents.EventSequence) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("EventSequence{Queue: %s, JobSetName: %s, Events: [", eventSequence.Queue, eventSequence.JobSetName))
	for i, event := range eventSequence.Events {
		sb.WriteString(EventSummary(event))
		if i != len(eventSequence.Events)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString("]}")
	return sb.String()
}

func EventSummary(event *armadaevents.EventSequence_Event) string {
	return strings.ReplaceAll(fmt.Sprintf("%T", event.Event), "*armadaevents.EventSequence_Event_", "")
}
