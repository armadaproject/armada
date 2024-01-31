package interfaces

import (
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// LegacySchedulerJob is the job interface used throughout the scheduler.
type LegacySchedulerJob interface {
	GetId() string
	GetQueue() string
	GetJobSet() string
	GetPerQueuePriority() uint32
	GetSubmitTime() time.Time
	GetAnnotations() map[string]string
	GetPodRequirements(priorityClasses map[string]types.PriorityClass) *schedulerobjects.PodRequirements
	GetPriorityClassName() string
	GetScheduledAtPriority() (int32, bool)
	GetNodeSelector() map[string]string
	GetAffinity() *v1.Affinity
	GetTolerations() []v1.Toleration
	GetResourceRequirements() v1.ResourceRequirements
	GetQueueTtlSeconds() int64
	// GetSchedulingKey returns (schedulingKey, true) if the job has a scheduling key associated with it and
	// (emptySchedulingKey, false) otherwise, where emptySchedulingKey is the zero value of the SchedulingKey type.
	GetSchedulingKey() (schedulerobjects.SchedulingKey, bool)
	// SchedulingOrderCompare defines the order in which jobs in a queue should be scheduled
	// (both when scheduling new jobs and when re-scheduling evicted jobs).
	// Specifically, compare returns
	//   - 0 if the jobs have equal job id,
	//   - -1 if job should be scheduled before other,
	//   - +1 if other should be scheduled before other.
	SchedulingOrderCompare(other LegacySchedulerJob) int
}

func PriorityClassFromLegacySchedulerJob(priorityClasses map[string]types.PriorityClass, defaultPriorityClassName string, job LegacySchedulerJob) types.PriorityClass {
	priorityClassName := job.GetPriorityClassName()
	if priorityClass, ok := priorityClasses[priorityClassName]; ok {
		return priorityClass
	}
	// We could return (types.PriorityClass{}, false) here, but then callers
	// might handle this situation in different ways; return the default
	// priority class in order to enforce uniformity.
	return priorityClasses[defaultPriorityClassName]
}

func SchedulingKeyFromLegacySchedulerJob(skg *schedulerobjects.SchedulingKeyGenerator, job LegacySchedulerJob) schedulerobjects.SchedulingKey {
	return skg.Key(
		job.GetNodeSelector(),
		job.GetAffinity(),
		job.GetTolerations(),
		job.GetResourceRequirements().Requests,
		job.GetPriorityClassName(),
	)
}
