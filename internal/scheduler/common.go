package scheduler

import (
	"fmt"

	"golang.org/x/exp/maps"

	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

type JobSummary struct {
	Summary string
	Verbose string
}

// JobsSummary returns a string giving an overview of the provided jobs meant for logging.
// For example: "affected queues [A, B]; resources {A: {cpu: 1}, B: {cpu: 2}}; jobs [jobAId, jobBId]".
func JobsSummary(jctxs []*schedulercontext.JobSchedulingContext) *JobSummary {
	if len(jctxs) == 0 {
		return nil
	}
	jobsByQueue := armadaslices.MapAndGroupByFuncs(
		jctxs,
		func(jctx *schedulercontext.JobSchedulingContext) string {
			return jctx.Job.GetQueue()
		},
		func(jctx *schedulercontext.JobSchedulingContext) interfaces.LegacySchedulerJob {
			return jctx.Job
		},
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
	jobCountPerQueue := armadamaps.MapValues(
		jobsByQueue,
		func(jobs []interfaces.LegacySchedulerJob) int {
			return len(jobs)
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
	return &JobSummary{
		Summary: fmt.Sprintf(
			"affected queues %v; resources %v; jobs per queue %v",
			maps.Keys(jobsByQueue),
			armadamaps.MapValues(
				resourcesByQueue,
				func(rl schedulerobjects.ResourceList) string {
					return rl.CompactString()
				},
			),
			jobCountPerQueue,
		),
		Verbose: fmt.Sprintf("affected jobs %v", jobIdsByQueue),
	}
}
