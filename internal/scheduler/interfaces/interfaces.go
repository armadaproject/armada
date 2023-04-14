package interfaces

import (
	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// LegacySchedulerJob is the job interface used throughout the scheduler.
//
// TODO: Rename to SchedulerJob.
type LegacySchedulerJob interface {
	GetId() string
	GetQueue() string
	GetJobSet() string
	GetAnnotations() map[string]string
	GetRequirements(map[string]configuration.PriorityClass) *schedulerobjects.JobSchedulingInfo
}
