package schedulerobjects

import (
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
)

// SchedulerResult is returned by Rescheduler.Schedule().
type SchedulerResult struct {
	// Running jobs that should be preempted.
	PreemptedJobs []interfaces.LegacySchedulerJob
	// Queued jobs that should be scheduled.
	ScheduledJobs []interfaces.LegacySchedulerJob
	// Queued jobs that could not be scheduled.
	// This is used to fail jobs that could not schedule above `minimumGangCardinality`.
	FailedJobs []interfaces.LegacySchedulerJob
	// For each preempted job, maps the job id to the id of the node on which the job was running.
	// For each scheduled job, maps the job id to the id of the node on which the job should be scheduled.
	NodeIdByJobId map[string]string
	// Each result may bundle the result of several scheduling decisions.
	// These are the corresponding scheduling contexts.
	// TODO: This doesn't seem like the right approach.
	SchedulingContexts []*schedulercontext.SchedulingContext
}

// PreemptedJobsFromSchedulerResult returns the slice of preempted jobs in the result cast to type T.
func PreemptedJobsFromSchedulerResult[T interfaces.LegacySchedulerJob](sr *SchedulerResult) []T {
	rv := make([]T, len(sr.PreemptedJobs))
	for i, job := range sr.PreemptedJobs {
		rv[i] = job.(T)
	}
	return rv
}

// ScheduledJobsFromSchedulerResult returns the slice of scheduled jobs in the result cast to type T.
func ScheduledJobsFromSchedulerResult[T interfaces.LegacySchedulerJob](sr *SchedulerResult) []T {
	rv := make([]T, len(sr.ScheduledJobs))
	for i, job := range sr.ScheduledJobs {
		rv[i] = job.(T)
	}
	return rv
}

// FailedJobsFromSchedulerResult returns the slice of scheduled jobs in the result cast to type T.
func FailedJobsFromSchedulerResult[T interfaces.LegacySchedulerJob](sr *SchedulerResult) []T {
	rv := make([]T, len(sr.FailedJobs))
	for i, job := range sr.FailedJobs {
		rv[i] = job.(T)
	}
	return rv
}
