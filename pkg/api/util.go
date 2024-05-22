package api

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func NodeIdFromExecutorAndNodeName(executor, nodeName string) string {
	return fmt.Sprintf("%s-%s", executor, nodeName)
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
	case *EventMessage_Preempted:
		return e.Preempted.JobSetId
	}
	return ""
}
