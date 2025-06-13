package simulator

import (
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/pointer"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func NodeTemplate32Cpu(n int64) *NodeTemplate {
	return &NodeTemplate{
		Number: n,
		TotalResources: &schedulerobjects.ResourceList{
			Resources: map[string]*resource.Quantity{
				"cpu":    pointer.MustParseResource("32"),
				"memory": pointer.MustParseResource("256Gi"),
			},
		},
	}
}

func NodeTemplateGpu(n int64) *NodeTemplate {
	return &NodeTemplate{
		Number: n,
		TotalResources: &schedulerobjects.ResourceList{
			Resources: map[string]*resource.Quantity{
				"cpu":            pointer.MustParseResource("128"),
				"memory":         pointer.MustParseResource("4096Gi"),
				"nvidia.com/gpu": pointer.MustParseResource("8"),
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
	jobTemplate.EarliestSubmitTime = protoutil.ToDuration(minSubmitTime)
	return jobTemplate
}

func JobTemplate32Cpu(n int64, jobSet, priorityClassName string) *JobTemplate {
	return &JobTemplate{
		Number:            n,
		JobSet:            jobSet,
		PriorityClassName: priorityClassName,
		Requirements: &schedulerobjects.PodRequirements{
			ResourceRequirements: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    resource.MustParse("32"),
					"memory": resource.MustParse("256Gi"),
				},
			},
		},

		RuntimeDistribution: &ShiftedExponential{Minimum: protoutil.ToDuration(1 * time.Minute)},
	}
}

func GangJobTemplate32Cpu(numJobs int64, gangCardinality uint32, jobSet, priorityClassName string) *JobTemplate {
	jobTemplate := JobTemplate32Cpu(numJobs, jobSet, priorityClassName)
	jobTemplate.GangCardinality = gangCardinality
	return jobTemplate
}

func JobTemplate1Cpu(n int64, jobSet, priorityClassName string) *JobTemplate {
	return &JobTemplate{
		Number:            n,
		JobSet:            jobSet,
		PriorityClassName: priorityClassName,
		Requirements: &schedulerobjects.PodRequirements{
			ResourceRequirements: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    resource.MustParse("1"),
					"memory": resource.MustParse("10Gi"),
				},
			},
		},
		RuntimeDistribution: &ShiftedExponential{Minimum: protoutil.ToDuration(time.Minute)},
	}
}

func RepeatEvents(n int, seq *armadaevents.EventSequence) *armadaevents.EventSequence {
	seq.Events = armadaslices.Repeat(n, seq.Events...)
	return seq
}

func SubmitJob(n int, queue string, jobSetName string) *armadaevents.EventSequence {
	seq := &armadaevents.EventSequence{
		Queue:      queue,
		JobSetName: jobSetName,
		Events: []*armadaevents.EventSequence_Event{
			{
				Event: &armadaevents.EventSequence_Event_SubmitJob{
					SubmitJob: &armadaevents.SubmitJob{},
				},
			},
		},
	}
	return RepeatEvents(n, seq)
}

func JobRunLeased(n int, queue string, jobSetName string) *armadaevents.EventSequence {
	seq := &armadaevents.EventSequence{
		Queue:      queue,
		JobSetName: jobSetName,
		Events: []*armadaevents.EventSequence_Event{
			{
				Event: &armadaevents.EventSequence_Event_JobRunLeased{
					JobRunLeased: &armadaevents.JobRunLeased{},
				},
			},
		},
	}
	return RepeatEvents(n, seq)
}

func JobRunPreempted(n int, queue string, jobSetName string) *armadaevents.EventSequence {
	seq := &armadaevents.EventSequence{
		Queue:      queue,
		JobSetName: jobSetName,
		Events: []*armadaevents.EventSequence_Event{
			{
				Event: &armadaevents.EventSequence_Event_JobRunPreempted{
					JobRunPreempted: &armadaevents.JobRunPreempted{},
				},
			},
		},
	}
	return RepeatEvents(n, seq)
}

func JobSucceeded(n int, queue string, jobSetName string) *armadaevents.EventSequence {
	seq := &armadaevents.EventSequence{
		Queue:      queue,
		JobSetName: jobSetName,
		Events: []*armadaevents.EventSequence_Event{
			{
				Event: &armadaevents.EventSequence_Event_JobSucceeded{
					JobSucceeded: &armadaevents.JobSucceeded{},
				},
			},
		},
	}
	return RepeatEvents(n, seq)
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
