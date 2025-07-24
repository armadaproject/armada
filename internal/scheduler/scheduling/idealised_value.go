package scheduling

import (
	"math"

	"golang.org/x/time/rate"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

// Rate limiter with an infinite limit. This is used because when dry-run scheduling, we don't want to apply rate limits
var noOpRateLimiter = rate.NewLimiter(rate.Inf, math.MaxInt)

// CalculateIdealisedValue adds an idealisedValue to every queue context in the scheduling context.  This as defined as follows:
//   - If the pool is market driven, this is the value of jobs that would be scheduled if all cluster resources  were contained on a single, giant node
//   - If the pool is not market driven then the value is zero.
func CalculateIdealisedValue(
	ctx *armadacontext.Context,
	sctx *schedulercontext.SchedulingContext,
	nodes []*internaltypes.Node,
	jobDb jobdb.JobRepository,
	constraints schedulerconstraints.SchedulingConstraints,
	floatingResourceTypes *floatingresources.FloatingResourceTypes,
	config configuration.SchedulingConfig,
	rlf *internaltypes.ResourceListFactory,
	resourceUnit internaltypes.ResourceList,
) error {
	// We only calculate optimum placement for market driven pools
	marketConfig := config.GetMarketConfig(sctx.Pool)
	if marketConfig == nil || !marketConfig.Enabled {
		return nil
	}

	// This is necessary to diable per round scheduling limits- e.g. maxFraction to schedule.
	permissiveSchedulingConstraints := schedulingConstraints{constraints}

	// Set up a duplicate scheduling context.  Note that we can't use the original scheduling context here because:
	// - it's mutable
	// - it is initialised with queue usage, whereas we want to schedule on an empty cluster
	dummySchedulingContext := schedulercontext.NewSchedulingContext(
		sctx.Pool,
		sctx.FairnessCostProvider,
		noOpRateLimiter,
		sctx.TotalResources)

	for _, qctx := range sctx.QueueSchedulingContexts {
		err := dummySchedulingContext.AddQueueSchedulingContext(
			qctx.Queue,
			qctx.Weight,
			qctx.RawWeight,
			map[string]internaltypes.ResourceList{},
			qctx.Demand,
			qctx.ConstrainedDemand,
			qctx.ConstrainedDemand,
			noOpRateLimiter)
		if err != nil {
			return err
		}
	}

	// set up nodes
	runningJobs := make([]*jobdb.Job, 0)
	for _, node := range nodes {
		for _, jobId := range node.GetRunningJobIds() {
			job := jobDb.GetById(jobId)
			if job != nil && job.LatestRun() != nil && job.LatestRun().Pool() == sctx.Pool {
				runningJobs = append(runningJobs, job)
			}
		}
	}
	optimalScheduler, err := NewIdealisedValueScheduler(
		dummySchedulingContext,
		permissiveSchedulingConstraints,
		floatingResourceTypes,
		config,
		rlf,
		nodes,
		jobDb,
		runningJobs,
	)
	if err != nil {
		return err
	}

	_, err = optimalScheduler.Schedule(ctx)
	if err != nil {
		return err
	}
	idealisedValues := valueFromSchedulingResult(dummySchedulingContext, resourceUnit)
	idealisedAllocations := allocationFromSchedulingResult(dummySchedulingContext)
	for _, qctx := range sctx.QueueSchedulingContexts {
		qctx.IdealisedValue = idealisedValues[qctx.Queue]
		idealisedAlloc, ok := idealisedAllocations[qctx.Queue]
		if !ok {
			idealisedAlloc = rlf.MakeAllZero()
		}
		qctx.IdealisedAllocated = idealisedAlloc
	}
	return nil
}

func valueFromSchedulingResult(sctx *schedulercontext.SchedulingContext, resourceUnit internaltypes.ResourceList) map[string]float64 {
	valueByQueue := make(map[string]float64, len(sctx.QueueSchedulingContexts))
	for _, qtx := range sctx.QueueSchedulingContexts {

		for _, j := range qtx.SuccessfulJobSchedulingContexts {
			price := j.Job.GetRawBidPrice(sctx.Pool)
			resourceUnits := j.Job.KubernetesResourceRequirements().DivideZeroOnError(resourceUnit).Max()
			valueByQueue[j.Job.Queue()] += price * resourceUnits
		}

		for _, j := range qtx.RescheduledJobSchedulingContexts {
			price := j.Job.GetRawBidPrice(sctx.Pool)
			resourceUnits := j.Job.KubernetesResourceRequirements().DivideZeroOnError(resourceUnit).Max()
			valueByQueue[j.Job.Queue()] += price * resourceUnits
		}
	}
	return valueByQueue
}

func allocationFromSchedulingResult(sctx *schedulercontext.SchedulingContext) map[string]internaltypes.ResourceList {
	allocationByQueue := make(map[string]internaltypes.ResourceList, len(sctx.QueueSchedulingContexts))
	for _, qtx := range sctx.QueueSchedulingContexts {
		allocationByQueue[qtx.Queue] = qtx.Allocated
	}
	return allocationByQueue
}

type schedulingConstraints struct {
	schedulerconstraints.SchedulingConstraints
}

func (s schedulingConstraints) CheckRoundConstraints(_ *schedulercontext.SchedulingContext) (bool, string, error) {
	return true, "", nil
}
