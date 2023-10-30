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
	// Returns (schedulingKey, true) if the job has a scheduling key associated with it.
	// Returns (emptySchedulingKey, false) otherwise, where emptySchedulingKey is the zero value of the SchedulingKey type.
	GetSchedulingKey() (schedulerobjects.SchedulingKey, bool)
}
