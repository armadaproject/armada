package scheduler

import (
	"fmt"
	"math"
	"strconv"
	"time"

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
	// Running jobs that should be preempted.
	PreemptedJobs []interfaces.LegacySchedulerJob
	// Queued jobs that should be scheduled.
	ScheduledJobs []interfaces.LegacySchedulerJob
	// For each preempted job, maps the job id to the id of the node on which the job was running.
	// For each scheduled job, maps the job id to the id of the node on which the job should be scheduled.
	NodeIdByJobId map[string]string
}

func NewSchedulerResult[S ~[]T, T interfaces.LegacySchedulerJob](
	preemptedJobs S,
	scheduledJobs S,
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
	return &SchedulerResult{
		PreemptedJobs: castPreemptedJobs,
		ScheduledJobs: castScheduledJobs,
		NodeIdByJobId: nodeIdByJobId,
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

func jobSchedulingContextsFromJobs[T interfaces.LegacySchedulerJob](jobs []T, executorId string, priorityClasses map[string]configuration.PriorityClass) []*schedulercontext.JobSchedulingContext {
	if jobs == nil {
		return nil
	}
	if len(jobs) == 0 {
		return make([]*schedulercontext.JobSchedulingContext, 0)
	}
	jctxs := make([]*schedulercontext.JobSchedulingContext, len(jobs))
	timestamp := time.Now()
	for i, job := range jobs {
		jctxs[i] = &schedulercontext.JobSchedulingContext{
			Created:    timestamp,
			ExecutorId: executorId,
			JobId:      job.GetId(),
			Job:        job,
			Req:        PodRequirementFromLegacySchedulerJob(job, priorityClasses),
		}
	}
	return jctxs
}

func isEvictedJob(job interfaces.LegacySchedulerJob) bool {
	return job.GetAnnotations()[schedulerconfig.IsEvictedAnnotation] == "true"
}

func targetNodeIdFromLegacySchedulerJob(job interfaces.LegacySchedulerJob) (string, bool) {
	nodeSelector := job.GetNodeSelector()
	if nodeSelector == nil {
		return "", false
	}
	nodeId, ok := nodeSelector[schedulerconfig.NodeIdLabel]
	return nodeId, ok
}

// GangIdAndCardinalityFromLegacySchedulerJob returns a tuple (gangId, gangCardinality, isGangJob, error).
func GangIdAndCardinalityFromLegacySchedulerJob(job interfaces.LegacySchedulerJob) (string, int, bool, error) {
	return GangIdAndCardinalityFromAnnotations(job.GetAnnotations())
}

// GangIdAndCardinalityFromAnnotations returns a tuple (gangId, gangCardinality, isGangJob, error).
func GangIdAndCardinalityFromAnnotations(annotations map[string]string) (string, int, bool, error) {
	if annotations == nil {
		return "", 0, false, nil
	}
	gangId, ok := annotations[configuration.GangIdAnnotation]
	if !ok {
		return "", 0, false, nil
	}
	gangCardinalityString, ok := annotations[configuration.GangCardinalityAnnotation]
	if !ok {
		return "", 0, false, errors.Errorf("missing annotation %s", configuration.GangCardinalityAnnotation)
	}
	gangCardinality, err := strconv.Atoi(gangCardinalityString)
	if err != nil {
		return "", 0, false, errors.WithStack(err)
	}
	if gangCardinality <= 0 {
		return "", 0, false, errors.Errorf("gang cardinality is non-positive %d", gangCardinality)
	}
	return gangId, gangCardinality, true, nil
}

// ResourceListAsWeightedMillis returns the linear combination of the milli values in rl with given weights.
// This function overflows for values that exceed MaxInt64. E.g., 1Pi is fine but not 10Pi.
func ResourceListAsWeightedMillis(weights map[string]float64, rl schedulerobjects.ResourceList) int64 {
	var rv int64
	for t, f := range weights {
		q := rl.Get(t)
		rv += int64(math.Round(float64(q.MilliValue()) * f))
	}
	return rv
}

func PodRequirementsFromLegacySchedulerJobs[S ~[]E, E interfaces.LegacySchedulerJob](jobs S, priorityClasses map[string]configuration.PriorityClass) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, len(jobs))
	for i, job := range jobs {
		rv[i] = PodRequirementFromLegacySchedulerJob(job, priorityClasses)
	}
	return rv
}

func PodRequirementFromLegacySchedulerJob[E interfaces.LegacySchedulerJob](job E, priorityClasses map[string]configuration.PriorityClass) *schedulerobjects.PodRequirements {
	annotations := make(map[string]string, len(configuration.ArmadaManagedAnnotations)+len(schedulerconfig.ArmadaSchedulerManagedAnnotations))
	for _, key := range configuration.ArmadaManagedAnnotations {
		if value, ok := job.GetAnnotations()[key]; ok {
			annotations[key] = value
		}
	}
	for _, key := range schedulerconfig.ArmadaSchedulerManagedAnnotations {
		if value, ok := job.GetAnnotations()[key]; ok {
			annotations[key] = value
		}
	}
	annotations[schedulerconfig.JobIdAnnotation] = job.GetId()
	annotations[schedulerconfig.QueueAnnotation] = job.GetQueue()
	info := job.GetRequirements(priorityClasses)
	req := PodRequirementFromJobSchedulingInfo(info)
	req.Annotations = annotations
	return req
}

func PodRequirementFromJobSchedulingInfo(info *schedulerobjects.JobSchedulingInfo) *schedulerobjects.PodRequirements {
	for _, oreq := range info.ObjectRequirements {
		if preq := oreq.GetPodRequirements(); preq != nil {
			return preq
		}
	}
	return nil
}
