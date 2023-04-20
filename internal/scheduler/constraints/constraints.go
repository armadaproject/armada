package constraints

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

const (
	UnschedulableReasonMaximumNumberOfJobsScheduled  = "maximum number of jobs scheduled"
	UnschedulableReasonMaximumNumberOfGangsScheduled = "maximum number of gangs scheduled"
)

// SchedulingConstraints contains scheduling constraints, e.g., per-queue resource limits.
type SchedulingConstraints struct {
	// Max number of jobs to scheduler per lease jobs call.
	MaximumJobsToSchedule uint
	// Max number of jobs to scheduler per lease jobs call.
	MaximumGangsToSchedule uint
	// Max number of jobs to consider for a queue before giving up.
	MaxLookbackPerQueue uint
	// Jobs leased to this executor must be at least this large.
	// Used, e.g., to avoid scheduling CPU-only jobs onto clusters with GPUs.
	MinimumJobSize schedulerobjects.ResourceList
	// Per-queue resource limits.
	// Map from resource type to the limit for that resource.
	//
	// TODO: Remove
	MaximalResourceFractionPerQueue map[string]float64
	// Scheduling constraints for specific priority classes.
	PriorityClassSchedulingConstraintsByPriorityClassName map[string]PriorityClassSchedulingConstraints
	// Max resources to schedule per queue at a time.
	MaximalResourceFractionToSchedulePerQueue map[string]float64
	// Max resources to schedule at a time.
	MaximalResourceFractionToSchedule map[string]float64
}

// PriorityClassSchedulingConstraints contains scheduling constraints that apply to jobs of a specific priority class.
type PriorityClassSchedulingConstraints struct {
	PriorityClassName     string
	PriorityClassPriority int32
	// Prevents jobs of this priority class from being scheduled if doing so would exceed
	// cumulative resource usage at priority priorityClassPriority for the queue the job originates from.
	//
	// Cumulative resource usage at priority x includes resources allocated to jobs of priorityClassPriority x or lower.
	MaximumCumulativeResourceFractionPerQueue map[string]float64
}

func SchedulingConstraintsFromSchedulingConfig(
	minimumJobSize schedulerobjects.ResourceList,
	config configuration.SchedulingConfig,
) SchedulingConstraints {
	priorityClassSchedulingConstraintsByPriorityClassName := make(map[string]PriorityClassSchedulingConstraints, len(config.Preemption.PriorityClasses))
	for name, priorityClass := range config.Preemption.PriorityClasses {
		priorityClassSchedulingConstraintsByPriorityClassName[name] = PriorityClassSchedulingConstraints{
			PriorityClassName:                         name,
			PriorityClassPriority:                     priorityClass.Priority,
			MaximumCumulativeResourceFractionPerQueue: priorityClass.MaximalResourceFractionPerQueue,
		}
	}
	return SchedulingConstraints{
		MaximumJobsToSchedule:           config.MaximumJobsToSchedule,
		MaximumGangsToSchedule:          config.MaximumGangsToSchedule,
		MaxLookbackPerQueue:             config.QueueLeaseBatchSize,
		MinimumJobSize:                  minimumJobSize,
		MaximalResourceFractionPerQueue: config.MaximalResourceFractionPerQueue,
		PriorityClassSchedulingConstraintsByPriorityClassName: priorityClassSchedulingConstraintsByPriorityClassName,
		MaximalResourceFractionToSchedulePerQueue:             config.MaximalResourceFractionToSchedulePerQueue,
		MaximalResourceFractionToSchedule:                     config.MaximalClusterFractionToSchedule,
	}
}

func (constraints *SchedulingConstraints) CheckGlobalConstraints(ctx context.Context, sctx *schedulercontext.SchedulingContext) (bool, string, error) {
	if constraints.MaximumJobsToSchedule != 0 && sctx.NumScheduledJobs > int(constraints.MaximumJobsToSchedule) {
		return false, UnschedulableReasonMaximumNumberOfJobsScheduled, nil
	}
	if constraints.MaximumGangsToSchedule != 0 && sctx.NumScheduledGangs > int(constraints.MaximumGangsToSchedule) {
		return false, UnschedulableReasonMaximumNumberOfGangsScheduled, nil
	}

	// MaximalResourceFractionToSchedule check.
	totalScheduledResources := sctx.ScheduledResourcesByPriority.AggregateByResource()
	if exceeded, reason := exceedsResourceLimits(
		ctx,
		totalScheduledResources,
		sctx.TotalResources,
		constraints.MaximalResourceFractionToSchedule,
	); exceeded {
		unschedulableReason := reason + " (overall per scheduling round limit)"
		return false, unschedulableReason, nil
	}

	return true, "", nil
}

func (constraints *SchedulingConstraints) CheckPerQueueAndPriorityClassConstraints(
	ctx context.Context,
	sctx *schedulercontext.SchedulingContext,
	queue string,
	priorityClassName string,
) (bool, string, error) {
	// We assume that all jobs in a gang have the same priority class (which we enforce at job submission).
	qctx := sctx.QueueSchedulingContexts[queue]
	if qctx == nil {
		return false, "", errors.Errorf("no QueueSchedulingContext for queue %s", queue)
	}

	// MaximalResourceFractionToSchedulePerQueue check.
	if exceeded, reason := exceedsResourceLimits(
		ctx,
		qctx.ScheduledResourcesByPriority.AggregateByResource(),
		sctx.TotalResources,
		constraints.MaximalResourceFractionToSchedulePerQueue,
	); exceeded {
		unschedulableReason := reason + " (per scheduling round limit for this queue)"
		return false, unschedulableReason, nil
	}

	// MaximalResourceFractionPerQueue check.
	if exceeded, reason := exceedsResourceLimits(
		ctx,
		qctx.ResourcesByPriority.AggregateByResource(),
		sctx.TotalResources,
		constraints.MaximalResourceFractionPerQueue,
	); exceeded {
		unschedulableReason := reason + " (total limit for this queue)"
		return false, unschedulableReason, nil
	}

	// MaximalCumulativeResourceFractionPerQueueAndPriority check.
	if priorityClassConstraint, ok := constraints.PriorityClassSchedulingConstraintsByPriorityClassName[priorityClassName]; ok {
		allocatedByPriorityAndResourceType := schedulerobjects.NewAllocatedByPriorityAndResourceType([]int32{priorityClassConstraint.PriorityClassPriority})
		for p, rl := range qctx.ResourcesByPriority {
			allocatedByPriorityAndResourceType.MarkAllocated(p, rl)
		}
		if exceeded, reason := exceedsResourceLimits(
			ctx,
			schedulerobjects.QuantityByPriorityAndResourceType(allocatedByPriorityAndResourceType).AggregateByResource(),
			sctx.TotalResources,
			priorityClassConstraint.MaximumCumulativeResourceFractionPerQueue,
		); exceeded {
			unschedulableReason := reason + fmt.Sprintf(" for priority class %s (total limit for this queue)", priorityClassName)
			return false, unschedulableReason, nil
		}
	}

	return true, "", nil
}

// exceedsResourceLimits returns true if used/total > limits for some resource t,
// and, if that is the case, a string indicating which resource limit was exceeded.
func exceedsResourceLimits(_ context.Context, used, total schedulerobjects.ResourceList, limits map[string]float64) (bool, string) {
	for resourceType, limit := range limits {
		totalAmount := total.Get(resourceType)
		usedAmount := used.Get(resourceType)
		if armadaresource.QuantityAsFloat64(usedAmount)/armadaresource.QuantityAsFloat64(totalAmount) > limit {
			return true, fmt.Sprintf("scheduling would exceed %s quota", resourceType)
		}
	}
	return false, ""
}
