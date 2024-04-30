package scheduler

import (
	"fmt"

	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// PrintJobSummary logs a summary of the job scheduling context
// It will log a high level summary at Info level, and a list of all queues + jobs affected at debug level
func PrintJobSummary(ctx *armadacontext.Context, prefix string, jctxs []*schedulercontext.JobSchedulingContext) {
	if len(jctxs) == 0 {
		return
	}
	jobsByQueue := armadaslices.MapAndGroupByFuncs(
		jctxs,
		func(jctx *schedulercontext.JobSchedulingContext) string {
			return jctx.Job.GetQueue()
		},
		func(jctx *schedulercontext.JobSchedulingContext) *jobdb.Job {
			return jctx.Job
		},
	)
	resourcesByQueue := armadamaps.MapValues(
		jobsByQueue,
		func(jobs []*jobdb.Job) schedulerobjects.ResourceList {
			rv := schedulerobjects.NewResourceListWithDefaultSize()
			for _, job := range jobs {
				rv.AddV1ResourceList(job.GetResourceRequirements().Requests)
			}
			return rv
		},
	)
	jobCountPerQueue := armadamaps.MapValues(
		jobsByQueue,
		func(jobs []*jobdb.Job) int {
			return len(jobs)
		},
	)
	jobIdsByQueue := armadamaps.MapValues(
		jobsByQueue,
		func(jobs []*jobdb.Job) []string {
			rv := make([]string, len(jobs))
			for i, job := range jobs {
				rv[i] = job.GetId()
			}
			return rv
		},
	)
	summary := fmt.Sprintf(
		"affected queues %v; resources %v; jobs per queue %v",
		maps.Keys(jobsByQueue),
		armadamaps.MapValues(
			resourcesByQueue,
			func(rl schedulerobjects.ResourceList) string {
				return rl.CompactString()
			},
		),
		jobCountPerQueue,
	)
	verbose := fmt.Sprintf("affected jobs %v", jobIdsByQueue)

	ctx.Infof("%s %s", prefix, summary)
	ctx.Debugf("%s %s", prefix, verbose)
}
