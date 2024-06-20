package scheduler

import (
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

// SchedulerResult is returned by Rescheduler.Schedule().
type SchedulerResult struct {
	// Running jobs that should be preempted.
	PreemptedJobs []*schedulercontext.JobSchedulingContext
	// Queued jobs that should be scheduled.
	ScheduledJobs []*schedulercontext.JobSchedulingContext
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

// PreemptedJobsFromSchedulerResult returns the slice of preempted jobs in the result.
func PreemptedJobsFromSchedulerResult(sr *SchedulerResult) []*jobdb.Job {
	rv := make([]*jobdb.Job, len(sr.PreemptedJobs))
	for i, jctx := range sr.PreemptedJobs {
		rv[i] = jctx.Job
	}
	return rv
}

// ScheduledJobsFromSchedulerResult returns the slice of scheduled jobs in the result.
func ScheduledJobsFromSchedulerResult(sr *SchedulerResult) []*jobdb.Job {
	rv := make([]*jobdb.Job, len(sr.ScheduledJobs))
	for i, jctx := range sr.ScheduledJobs {
		rv[i] = jctx.Job
	}
	return rv
}
