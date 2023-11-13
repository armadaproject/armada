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
