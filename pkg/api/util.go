package api

import (
	"fmt"
	math "math"
	"strings"
	time "time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/logging"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/common/types"
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
		Executor:                         executor,
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

func (job *Job) GetPerQueuePriority() uint32 {
	priority := job.Priority
	if priority < 0 {
		return 0
	}
	if priority > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(math.Round(priority))
}

func (job *Job) GetSubmitTime() time.Time {
	return job.Created
}

func (job *Job) GetPodRequirements(priorityClasses map[string]types.PriorityClass) *schedulerobjects.PodRequirements {
	podSpec := job.GetMainPodSpec()

	priority, ok := PriorityFromPodSpec(podSpec, priorityClasses)
	if priorityClasses != nil && !ok {
		// Ignore this error if priorityByPriorityClassName is explicitly set to nil.
		// We assume that in this case the caller is sure the priority does not need to be set.
		err := errors.Errorf("unknown priorityClassName %s", podSpec.PriorityClassName)
		logging.WithStacktrace(logrus.NewEntry(logrus.New()), err).Error("failed to get priority from priorityClassName")
	}

	preemptionPolicy := string(v1.PreemptLowerPriority)
	if podSpec.PreemptionPolicy != nil {
		preemptionPolicy = string(*podSpec.PreemptionPolicy)
	}

	return &schedulerobjects.PodRequirements{
		NodeSelector:         podSpec.NodeSelector,
		Affinity:             podSpec.Affinity,
		Tolerations:          podSpec.Tolerations,
		Annotations:          maps.Clone(job.Annotations),
		Priority:             priority,
		PreemptionPolicy:     preemptionPolicy,
		ResourceRequirements: job.GetResourceRequirements(),
	}
}

// SchedulingResourceRequirementsFromPodSpec returns resource requests and limits necessary for scheduling a pod.
// The requests and limits are set to:
//
// max(
//
//	sum across all containers,
//	max over all init containers,
//
// )
//
// This is because containers run in parallel, whereas initContainers run serially.
func SchedulingResourceRequirementsFromPodSpec(podSpec *v1.PodSpec) v1.ResourceRequirements {
	rv := v1.ResourceRequirements{
		Requests: make(v1.ResourceList),
		Limits:   make(v1.ResourceList),
	}
	for _, c := range podSpec.Containers {
		for t, request := range c.Resources.Requests {
			q := rv.Requests[t]
			q.Add(request)
			rv.Requests[t] = q
		}
		for t, limit := range c.Resources.Limits {
			q := rv.Limits[t]
			q.Add(limit)
			rv.Limits[t] = q
		}
	}
	for _, c := range podSpec.InitContainers {
		for t, request := range c.Resources.Requests {
			if request.Cmp(rv.Requests[t]) == 1 {
				rv.Requests[t] = request
			}
		}
		for t, limit := range c.Resources.Limits {
			if limit.Cmp(rv.Limits[t]) == 1 {
				rv.Limits[t] = limit
			}
		}
	}
	return rv
}

// PriorityFromPodSpec returns the priority in a pod spec.
// If priority is set directly, that value is returned.
// Otherwise, it returns the value of the key podSpec.
// In both cases the value along with true boolean is returned.
// PriorityClassName in priorityByPriorityClassName map.
// If no priority is set for the pod spec, 0 along with a false boolean would be returned
func PriorityFromPodSpec(podSpec *v1.PodSpec, priorityClasses map[string]types.PriorityClass) (int32, bool) {
	// If there's no podspec there's nothing we can do
	if podSpec == nil {
		return 0, false
	}

	// If a priority is directly specified, use that
	if podSpec.Priority != nil {
		return *podSpec.Priority, true
	}

	// If we find a priority class use that
	priorityClass, ok := priorityClasses[podSpec.PriorityClassName]
	if ok {
		return priorityClass.Priority, true
	}

	// Couldn't find anything
	return 0, false
}

func (job *Job) GetPriorityClassName() string {
	podSpec := job.GetMainPodSpec()
	return podSpec.PriorityClassName
}

func (job *Job) GetNodeSelector() map[string]string {
	podSpec := job.GetMainPodSpec()
	return podSpec.NodeSelector
}

func (job *Job) GetAffinity() *v1.Affinity {
	podSpec := job.GetMainPodSpec()
	return podSpec.Affinity
}

func (job *Job) GetTolerations() []v1.Toleration {
	podSpec := job.GetMainPodSpec()
	return podSpec.Tolerations
}

func (job *Job) GetResourceRequirements() v1.ResourceRequirements {
	// Use pre-computed schedulingResourceRequirements if available.
	// Otherwise compute it from the containers in podSpec.
	podSpec := job.GetMainPodSpec()
	if len(job.SchedulingResourceRequirements.Requests) > 0 || len(job.SchedulingResourceRequirements.Limits) > 0 {
		return job.SchedulingResourceRequirements
	} else {
		return SchedulingResourceRequirementsFromPodSpec(podSpec)
	}
}

func (job *Job) GetJobSet() string {
	return job.JobSetId
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

func (job *JobSubmitRequestItem) GetMainPodSpec() *v1.PodSpec {
	if job.PodSpec != nil {
		return job.PodSpec
	} else if len(job.PodSpecs) > 0 {
		return job.PodSpecs[0]
	}
	return nil
}

func (job *Job) TotalResourceRequest() armadaresource.ComputeResources {
	podSpec := job.GetMainPodSpec()
	return armadaresource.TotalPodResourceRequest(podSpec)
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
