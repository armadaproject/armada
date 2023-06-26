package interfaces

import (
	v1 "k8s.io/api/core/v1"

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
	GetPriorityClassName() string
	GetNodeSelector() map[string]string
	GetAffinity() *v1.Affinity
	GetTolerations() []v1.Toleration
	GetResourceRequirements() v1.ResourceRequirements
}
