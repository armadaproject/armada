package api

import (
	"fmt"
	math "math"
	"strings"
	time "time"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/scheduler/adapters"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// IsTerminal returns true if the JobState s corresponds to a state
// that indicates the job has been terminated.
func (s JobState) IsTerminal() bool {
	switch s {
	case JobState_SUCCEEDED:
		return true
	case JobState_FAILED:
		return true
	}
	return false
}

func NewNodeFromNodeInfo(nodeInfo *NodeInfo, executor string, allowedPriorities []int32, lastSeen time.Time) (*schedulerobjects.Node, error) {
	if executor == "" {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "executor",
			Value:   executor,
			Message: "executor is empty",
		})
	}
	if nodeInfo.Name == "" {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "nodeInfo.Name",
			Value:   nodeInfo.Name,
			Message: "nodeInfo.Name is empty",
		})
	}

	allocatableByPriorityAndResource := schedulerobjects.NewAllocatableByPriorityAndResourceType(
		allowedPriorities,
		schedulerobjects.ResourceList{
			Resources: nodeInfo.TotalResources,
		},
	)
	for p, rl := range nodeInfo.NonArmadaAllocatedResources {
		allocatableByPriorityAndResource.MarkAllocated(p, schedulerobjects.ResourceList{Resources: rl.Resources})
	}
	nonArmadaAllocatedResources := make(map[int32]schedulerobjects.ResourceList)
	for p, rl := range nodeInfo.NonArmadaAllocatedResources {
		nonArmadaAllocatedResources[p] = schedulerobjects.ResourceList{Resources: rl.Resources}
	}
	resourceUsageByQueue := make(map[string]*schedulerobjects.ResourceList)
	for queueName, resourceUsage := range nodeInfo.ResourceUsageByQueue {
		resourceUsageByQueue[queueName] = &schedulerobjects.ResourceList{Resources: resourceUsage.Resources}
	}

	jobRunsByState := make(map[string]schedulerobjects.JobRunState)
	for jobId, state := range nodeInfo.RunIdsByState {
		jobRunsByState[jobId] = JobRunStateFromApiJobState(state)
	}
	return &schedulerobjects.Node{
		Id:                               fmt.Sprintf("%s-%s", executor, nodeInfo.Name),
		Name:                             nodeInfo.Name,
		LastSeen:                         lastSeen,
		Taints:                           nodeInfo.GetTaints(),
		Labels:                           nodeInfo.GetLabels(),
		TotalResources:                   schedulerobjects.ResourceList{Resources: nodeInfo.TotalResources},
		AllocatableByPriorityAndResource: allocatableByPriorityAndResource,
		NonArmadaAllocatedResources:      nonArmadaAllocatedResources,
		StateByJobRunId:                  jobRunsByState,
		Unschedulable:                    nodeInfo.Unschedulable,
		ResourceUsageByQueue:             resourceUsageByQueue,
		ReportingNodeType:                nodeInfo.NodeType,
	}, nil
}

func JobRunStateFromApiJobState(s JobState) schedulerobjects.JobRunState {
	switch s {
	case JobState_QUEUED:
		return schedulerobjects.JobRunState_UNKNOWN
	case JobState_PENDING:
		return schedulerobjects.JobRunState_PENDING
	case JobState_RUNNING:
		return schedulerobjects.JobRunState_RUNNING
	case JobState_SUCCEEDED:
		return schedulerobjects.JobRunState_SUCCEEDED
	case JobState_FAILED:
		return schedulerobjects.JobRunState_FAILED
	case JobState_UNKNOWN:
		return schedulerobjects.JobRunState_UNKNOWN
	}
	return schedulerobjects.JobRunState_UNKNOWN
}

func NewNodeTypeFromNodeInfo(nodeInfo *NodeInfo, indexedTaints map[string]interface{}, indexedLabels map[string]interface{}) *schedulerobjects.NodeType {
	return schedulerobjects.NewNodeType(nodeInfo.GetTaints(), nodeInfo.GetLabels(), indexedTaints, indexedLabels)
}

func (job *Job) GetRequirements(priorityClasses map[string]configuration.PriorityClass) *schedulerobjects.JobSchedulingInfo {
	podSpecs := job.GetAllPodSpecs()
	objectRequirements := make([]*schedulerobjects.ObjectRequirements, len(podSpecs))
	for i, podSpec := range podSpecs {
		if podSpec == nil {
			continue
		}
		objectRequirements[i] = &schedulerobjects.ObjectRequirements{
			Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
				PodRequirements: adapters.PodRequirementsFromPod(
					&v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: job.Annotations,
						},
						Spec: *podSpec,
					},
					priorityClasses,
				),
			},
		}
	}
	priorityClassName := ""
	if len(podSpecs) > 0 {
		priorityClassName = podSpecs[0].PriorityClassName
	}
	return &schedulerobjects.JobSchedulingInfo{
		PriorityClassName:  priorityClassName,
		Priority:           LogSubmitPriorityFromApiPriority(job.GetPriority()),
		SubmitTime:         job.GetCreated(),
		ObjectRequirements: objectRequirements,
	}
}

func (job *Job) GetJobSet() string {
	return job.JobSetId
}

// LogSubmitPriorityFromApiPriority returns the uint32 representation of the priority included with a submitted job,
// or an error if the conversion fails.
func LogSubmitPriorityFromApiPriority(priority float64) uint32 {
	if priority < 0 {
		priority = 0
	}
	if priority > math.MaxUint32 {
		priority = math.MaxUint32
	}
	priority = math.Round(priority)
	return uint32(priority)
}

func (job *Job) GetMainPodSpec() *v1.PodSpec {
	if job.PodSpec != nil {
		return job.PodSpec
	}
	for _, podSpec := range job.PodSpecs {
		if podSpec != nil {
			return podSpec
		}
	}
	return nil
}

func (job *Job) TotalResourceRequest() armadaresource.ComputeResources {
	totalResources := make(armadaresource.ComputeResources)
	for _, podSpec := range job.GetAllPodSpecs() {
		podResource := armadaresource.TotalPodResourceRequest(podSpec)
		totalResources.Add(podResource)
	}
	return totalResources
}

func ShortStringFromEventMessages(msgs []*EventMessage) string {
	var sb strings.Builder
	sb.WriteString("[")
	for i, msg := range msgs {
		sb.WriteString(msg.ShortString())
		if i < len(msgs)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString("]")
	return sb.String()
}

func (msg *EventMessage) ShortString() string {
	return strings.ReplaceAll(fmt.Sprintf("%T", msg.Events), "*api.EventMessage_", "")
}

func (testSpec *TestSpec) ShortString() string {
	var sb strings.Builder
	sb.WriteString(
		fmt.Sprintf(
			"%s: {queue: %s, job set: %s, timeout: %s, expected: [",
			testSpec.Name, testSpec.Queue, testSpec.JobSetId, testSpec.Timeout.String(),
		),
	)
	for i, e := range testSpec.GetExpectedEvents() {
		sb.WriteString(e.ShortString())
		if i < len(testSpec.GetExpectedEvents())-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString("]}")
	return sb.String()
}

func JobIdFromApiEvent(msg *EventMessage) string {
	switch e := msg.Events.(type) {
	case *EventMessage_Submitted:
		return e.Submitted.JobId
	case *EventMessage_Queued:
		return e.Queued.JobId
	case *EventMessage_DuplicateFound:
		return e.DuplicateFound.JobId
	case *EventMessage_Leased:
		return e.Leased.JobId
	case *EventMessage_LeaseReturned:
		return e.LeaseReturned.JobId
	case *EventMessage_LeaseExpired:
		return e.LeaseExpired.JobId
	case *EventMessage_Pending:
		return e.Pending.JobId
	case *EventMessage_Running:
		return e.Running.JobId
	case *EventMessage_UnableToSchedule:
		return e.UnableToSchedule.JobId
	case *EventMessage_Failed:
		return e.Failed.JobId
	case *EventMessage_Succeeded:
		return e.Succeeded.JobId
	case *EventMessage_Reprioritized:
		return e.Reprioritized.JobId
	case *EventMessage_Cancelling:
		return e.Cancelling.JobId
	case *EventMessage_Cancelled:
		return e.Cancelled.JobId
	case *EventMessage_Terminated:
		return e.Terminated.JobId
	case *EventMessage_Utilisation:
		return e.Utilisation.JobId
	case *EventMessage_IngressInfo:
		return e.IngressInfo.JobId
	case *EventMessage_Reprioritizing:
		return e.Reprioritizing.JobId
	case *EventMessage_Updated:
		return e.Updated.JobId
	case *EventMessage_Preempted:
		return e.Preempted.JobId
	}
	return ""
}

func JobSetIdFromApiEvent(msg *EventMessage) string {
	switch e := msg.Events.(type) {
	case *EventMessage_Submitted:
		return e.Submitted.JobSetId
	case *EventMessage_Queued:
		return e.Queued.JobSetId
	case *EventMessage_DuplicateFound:
		return e.DuplicateFound.JobSetId
	case *EventMessage_Leased:
		return e.Leased.JobSetId
	case *EventMessage_LeaseReturned:
		return e.LeaseReturned.JobSetId
	case *EventMessage_LeaseExpired:
		return e.LeaseExpired.JobSetId
	case *EventMessage_Pending:
		return e.Pending.JobSetId
	case *EventMessage_Running:
		return e.Running.JobSetId
	case *EventMessage_UnableToSchedule:
		return e.UnableToSchedule.JobSetId
	case *EventMessage_Failed:
		return e.Failed.JobSetId
	case *EventMessage_Succeeded:
		return e.Succeeded.JobSetId
	case *EventMessage_Reprioritized:
		return e.Reprioritized.JobSetId
	case *EventMessage_Cancelling:
		return e.Cancelling.JobSetId
	case *EventMessage_Cancelled:
		return e.Cancelled.JobSetId
	case *EventMessage_Terminated:
		return e.Terminated.JobSetId
	case *EventMessage_Utilisation:
		return e.Utilisation.JobSetId
	case *EventMessage_IngressInfo:
		return e.IngressInfo.JobSetId
	case *EventMessage_Reprioritizing:
		return e.Reprioritizing.JobSetId
	case *EventMessage_Updated:
		return e.Updated.JobSetId
	case *EventMessage_Preempted:
		return e.Preempted.JobSetId
	}
	return ""
}
