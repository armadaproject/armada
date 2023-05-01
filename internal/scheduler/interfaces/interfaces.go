package interfaces

import (
	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// LegacySchedulerJob is the job interface used throughout the scheduler.
type LegacySchedulerJob interface {
	GetId() string
	GetQueue() string
	GetJobSet() string
	GetAnnotations() map[string]string
	GetRequirements(map[string]configuration.PriorityClass) *schedulerobjects.JobSchedulingInfo
}

func PodRequirementFromLegacySchedulerJob(job LegacySchedulerJob, priorityClasses map[string]configuration.PriorityClass) *schedulerobjects.PodRequirements {
	schedulingInfo := job.GetRequirements(priorityClasses)
	if schedulingInfo == nil {
		return nil
	}
	for _, objectReq := range schedulingInfo.ObjectRequirements {
		if req := objectReq.GetPodRequirements(); req != nil {
			return req
		}
	}
	return nil
}
