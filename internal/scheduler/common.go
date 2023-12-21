package scheduler

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// JobsSummary returns a string giving an overview of the provided jobs meant for logging.
// For example: "affected queues [A, B]; resources {A: {cpu: 1}, B: {cpu: 2}}; jobs [jobAId, jobBId]".
func JobsSummary(jctxs []*schedulercontext.JobSchedulingContext) string {
	if len(jctxs) == 0 {
		return ""
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
		if gangMinimumCardinality > gangCardinality {
			return "", 1, 1, false, errors.Errorf("gang minimum cardinality %d cannot be greater than gang cardinality %d", gangMinimumCardinality, gangCardinality)
		}
		return gangId, gangCardinality, gangMinimumCardinality, true, nil
	}
}
