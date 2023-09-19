package scheduler

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// SchedulerResult is returned by Rescheduler.Schedule().
type SchedulerResult struct {
	// Whether the scheduler failed to create a result for some reason
	EmptyResult bool
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
	// The Scheduling Context. Being passed up for metrics decisions made in scheduler.go and scheduler_metrics.go.
	// Passing a pointer as the structure is enormous
	SchedulingContexts []*schedulercontext.SchedulingContext
}

func NewSchedulerResultForTest[S ~[]T, T interfaces.LegacySchedulerJob](
	preemptedJobs S,
	scheduledJobs S,
	failedJobs S,
	nodeIdByJobId map[string]string,
) *SchedulerResult {
	castPreemptedJobs := make([]interfaces.LegacySchedulerJob, len(preemptedJobs))
	for i, job := range preemptedJobs {
		castPreemptedJobs[i] = job
	}
	castScheduledJobs := make([]interfaces.LegacySchedulerJob, len(scheduledJobs))
	for i, job := range scheduledJobs {
		castScheduledJobs[i] = job
	}
	castFailedJobs := make([]interfaces.LegacySchedulerJob, len(failedJobs))
	for i, job := range failedJobs {
		castFailedJobs[i] = job
	}
	return &SchedulerResult{
		PreemptedJobs: castPreemptedJobs,
		ScheduledJobs: castScheduledJobs,
		NodeIdByJobId: nodeIdByJobId,
		FailedJobs:    castFailedJobs,
	}
}

// PreemptedJobsFromSchedulerResult returns the slice of preempted jobs in the result,
// cast to type T.
func PreemptedJobsFromSchedulerResult[T interfaces.LegacySchedulerJob](sr *SchedulerResult) []T {
	rv := make([]T, len(sr.PreemptedJobs))
	for i, job := range sr.PreemptedJobs {
		rv[i] = job.(T)
	}
	return rv
}

// ScheduledJobsFromScheduleResult returns the slice of scheduled jobs in the result,
// cast to type T.
func ScheduledJobsFromSchedulerResult[T interfaces.LegacySchedulerJob](sr *SchedulerResult) []T {
	rv := make([]T, len(sr.ScheduledJobs))
	for i, job := range sr.ScheduledJobs {
		rv[i] = job.(T)
	}
	return rv
}

// FailedJobsFromScheduleResult returns the slice of scheduled jobs in the result,
// cast to type T.
func FailedJobsFromSchedulerResult[T interfaces.LegacySchedulerJob](sr *SchedulerResult) []T {
	rv := make([]T, len(sr.FailedJobs))
	for i, job := range sr.FailedJobs {
		rv[i] = job.(T)
	}
	return rv
}

// JobsSummary returns a string giving an overview of the provided jobs meant for logging.
// For example: "affected queues [A, B]; resources {A: {cpu: 1}, B: {cpu: 2}}; jobs [jobAId, jobBId]".
func JobsSummary(jobs []interfaces.LegacySchedulerJob) string {
	if len(jobs) == 0 {
		return ""
	}
	jobsByQueue := armadaslices.GroupByFunc(
		jobs,
		func(job interfaces.LegacySchedulerJob) string { return job.GetQueue() },
	)
	resourcesByQueue := armadamaps.MapValues(
		jobsByQueue,
		func(jobs []interfaces.LegacySchedulerJob) schedulerobjects.ResourceList {
			rv := schedulerobjects.NewResourceListWithDefaultSize()
			for _, job := range jobs {
				rv.AddV1ResourceList(job.GetResourceRequirements().Requests)
			}
			return rv
		},
	)
	jobIdsByQueue := armadamaps.MapValues(
		jobsByQueue,
		func(jobs []interfaces.LegacySchedulerJob) []string {
			rv := make([]string, len(jobs))
			for i, job := range jobs {
				rv[i] = job.GetId()
			}
			return rv
		},
	)
	return fmt.Sprintf(
		"affected queues %v; resources %v; jobs %v",
		maps.Keys(jobsByQueue),
		armadamaps.MapValues(
			resourcesByQueue,
			func(rl schedulerobjects.ResourceList) string {
				return rl.CompactString()
			},
		),
		jobIdsByQueue,
	)
}

func isEvictedJob(job interfaces.LegacySchedulerJob) bool {
	return job.GetAnnotations()[schedulerconfig.IsEvictedAnnotation] == "true"
}

func targetNodeIdFromNodeSelector(nodeSelector map[string]string) (string, bool) {
	nodeId, ok := nodeSelector[schedulerconfig.NodeIdLabel]
	return nodeId, ok
}

// GangIdAndCardinalityFromLegacySchedulerJob returns a tuple (gangId, gangCardinality, gangMinimumCardinality, isGangJob, error).
func GangIdAndCardinalityFromLegacySchedulerJob(job interfaces.LegacySchedulerJob) (string, int, int, bool, error) {
	return GangIdAndCardinalityFromAnnotations(job.GetAnnotations())
}

// GangIdAndCardinalityFromAnnotations returns a tuple (gangId, gangCardinality, gangMinimumCardinality, isGangJob, error).
func GangIdAndCardinalityFromAnnotations(annotations map[string]string) (string, int, int, bool, error) {
	if annotations == nil {
		return "", 1, 1, false, nil
	}
	gangId, ok := annotations[configuration.GangIdAnnotation]
	if !ok {
		return "", 1, 1, false, nil
	}
	gangCardinalityString, ok := annotations[configuration.GangCardinalityAnnotation]
	if !ok {
		return "", 1, 1, false, errors.Errorf("missing annotation %s", configuration.GangCardinalityAnnotation)
	}
	gangCardinality, err := strconv.Atoi(gangCardinalityString)
	if err != nil {
		return "", 1, 1, false, errors.WithStack(err)
	}
	if gangCardinality <= 0 {
		return "", 1, 1, false, errors.Errorf("gang cardinality is non-positive %d", gangCardinality)
	}
	gangMinimumCardinalityString, ok := annotations[configuration.GangMinimumCardinalityAnnotation]
	if !ok {
		// If this is not set, default the minimum gang size to gangCardinality
		return gangId, gangCardinality, gangCardinality, true, nil
	} else {
		gangMinimumCardinality, err := strconv.Atoi(gangMinimumCardinalityString)
		if err != nil {
			return "", 1, 1, false, errors.WithStack(err)
		}
		if gangMinimumCardinality <= 0 {
			return "", 1, 1, false, errors.Errorf("gang minimum cardinality is non-positive %d", gangMinimumCardinality)
		}
		return gangId, gangCardinality, gangMinimumCardinality, true, nil
	}
}
