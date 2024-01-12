package scheduler

import (
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
)

// SchedulerResult is returned by Rescheduler.Schedule().
type SchedulerResult struct {
	// Running jobs that should be preempted.
	PreemptedJobs []*schedulercontext.JobSchedulingContext
	// Queued jobs that should be scheduled.
	ScheduledJobs []*schedulercontext.JobSchedulingContext
	// Queued jobs that could not be scheduled.
	// This is used to fail jobs that could not schedule above `minimumGangCardinality`.
	FailedJobs []*schedulercontext.JobSchedulingContext
	// For each preempted job, maps the job id to the id of the node on which the job was running.
	// For each scheduled job, maps the job id to the id of the node on which the job should be scheduled.
	NodeIdByJobId map[string]string
	// Each result may bundle the result of several scheduling decisions.
	// These are the corresponding scheduling contexts.
	// TODO: This doesn't seem like the right approach.
	SchedulingContexts []*schedulercontext.SchedulingContext
	// Additional annotations to be appended to the PodSpec.
	// Format: JobId -> AnnotationName -> AnnotationValue.
	AdditionalAnnotationsByJobId map[string]map[string]string
}

// PreemptedJobsFromSchedulerResult returns the slice of preempted jobs in the result cast to type T.
func PreemptedJobsFromSchedulerResult[T interfaces.LegacySchedulerJob](sr *SchedulerResult) []T {
	rv := make([]T, len(sr.PreemptedJobs))
	for i, jctx := range sr.PreemptedJobs {
		rv[i] = jctx.Job.(T)
	}
	return rv
}

// ScheduledJobsFromSchedulerResult returns the slice of scheduled jobs in the result cast to type T.
func ScheduledJobsFromSchedulerResult[T interfaces.LegacySchedulerJob](sr *SchedulerResult) []T {
	rv := make([]T, len(sr.ScheduledJobs))
	for i, jctx := range sr.ScheduledJobs {
		rv[i] = jctx.Job.(T)
	}
	return rv
}

// FailedJobsFromSchedulerResult returns the slice of scheduled jobs in the result cast to type T.
func FailedJobsFromSchedulerResult[T interfaces.LegacySchedulerJob](sr *SchedulerResult) []T {
	rv := make([]T, len(sr.FailedJobs))
	for i, jctx := range sr.FailedJobs {
		rv[i] = jctx.Job.(T)
	}
	return rv
}
