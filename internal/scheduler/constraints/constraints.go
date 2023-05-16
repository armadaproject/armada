package constraints

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

const (
	UnschedulableReasonMaximumResourcesScheduled     = "maximum resources scheduled"
	UnschedulableReasonMaximumNumberOfJobsScheduled  = "maximum number of jobs scheduled"
	UnschedulableReasonMaximumNumberOfGangsScheduled = "maximum number of gangs scheduled"
)

// IsTerminalUnschedulableReason returns true if reason indicates it's not possible to schedule any more jobs in this round.
func IsTerminalUnschedulableReason(reason string) bool {
	if reason == UnschedulableReasonMaximumNumberOfJobsScheduled {
		return true
	}
	if reason == UnschedulableReasonMaximumNumberOfJobsScheduled {
		return true
	}
	if reason == UnschedulableReasonMaximumNumberOfGangsScheduled {
		return true
	}
	return false
}

// SchedulingConstraints contains scheduling constraints, e.g., per-queue resource limits.
type SchedulingConstraints struct {
	// Max number of jobs to scheduler per lease jobs call.
	MaximumJobsToSchedule uint
	// Max number of jobs to scheduler per lease jobs call.
	MaximumGangsToSchedule uint
	// Max number of jobs to consider for a queue before giving up.
	MaxQueueLookback uint
	// Jobs leased to this executor must be at least this large.
	// Used, e.g., to avoid scheduling CPU-only jobs onto clusters with GPUs.
	MinimumJobSize schedulerobjects.ResourceList
	// Scheduling constraints for specific priority classes.
	PriorityClassSchedulingConstraintsByPriorityClassName map[string]PriorityClassSchedulingConstraints
	// Limits fraction of total resources scheduled per invocation.
	MaximumResourceFractionToSchedule map[string]float64
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
	pool string,
	minimumJobSize schedulerobjects.ResourceList,
	config configuration.SchedulingConfig,
) SchedulingConstraints {
	priorityClassSchedulingConstraintsByPriorityClassName := make(map[string]PriorityClassSchedulingConstraints, len(config.Preemption.PriorityClasses))
	for name, priorityClass := range config.Preemption.PriorityClasses {
		priorityClassSchedulingConstraintsByPriorityClassName[name] = PriorityClassSchedulingConstraints{
			PriorityClassName:                         name,
			PriorityClassPriority:                     priorityClass.Priority,
			MaximumCumulativeResourceFractionPerQueue: priorityClass.MaximumResourceFractionPerQueue,
		}
	}
	maximumResourceFractionToSchedule := config.MaximumResourceFractionToSchedule
	if limit, ok := config.MaximumResourceFractionToScheduleByPool[pool]; ok {
		maximumResourceFractionToSchedule = limit
	}
	return SchedulingConstraints{
		MaximumJobsToSchedule:             config.MaximumJobsToSchedule,
		MaximumGangsToSchedule:            config.MaximumGangsToSchedule,
		MaxQueueLookback:                  config.MaxQueueLookback,
		MinimumJobSize:                    minimumJobSize,
		MaximumResourceFractionToSchedule: maximumResourceFractionToSchedule,
		PriorityClassSchedulingConstraintsByPriorityClassName: priorityClassSchedulingConstraintsByPriorityClassName,
	}
}

func (constraints *SchedulingConstraints) CheckRoundConstraints(sctx *schedulercontext.SchedulingContext) (bool, string, error) {
	// MaximumJobsToSchedule check.
	if constraints.MaximumJobsToSchedule != 0 && sctx.NumScheduledJobs == int(constraints.MaximumJobsToSchedule) {
		return false, UnschedulableReasonMaximumNumberOfJobsScheduled, nil
	}

	// MaximumGangsToSchedule check.
	if constraints.MaximumGangsToSchedule != 0 && sctx.NumScheduledGangs == int(constraints.MaximumGangsToSchedule) {
		return false, UnschedulableReasonMaximumNumberOfGangsScheduled, nil
	}

	// MaximumResourceFractionToSchedule check.
	totalScheduledResources := sctx.ScheduledResourcesByPriority.AggregateByResource()
	if exceeded, _ := exceedsResourceLimits(
		totalScheduledResources,
		sctx.TotalResources,
		constraints.MaximumResourceFractionToSchedule,
	); exceeded {
		return false, UnschedulableReasonMaximumResourcesScheduled, nil
	}
	return true, "", nil
}

func (constraints *SchedulingConstraints) CheckPerQueueAndPriorityClassConstraints(
	sctx *schedulercontext.SchedulingContext,
	queue string,
	priorityClassName string,
) (bool, string, error) {
	qctx := sctx.QueueSchedulingContexts[queue]
	if qctx == nil {
		return false, "", errors.Errorf("no QueueSchedulingContext for queue %s", queue)
	}

	// PriorityClassSchedulingConstraintsByPriorityClassName check.
	if priorityClassConstraint, ok := constraints.PriorityClassSchedulingConstraintsByPriorityClassName[priorityClassName]; ok {
		allocatedByPriorityAndResourceType := schedulerobjects.NewAllocatedByPriorityAndResourceType([]int32{priorityClassConstraint.PriorityClassPriority})
		for p, rl := range qctx.AllocatedByPriority {
			allocatedByPriorityAndResourceType.MarkAllocated(p, rl)
		}
		if exceeded, reason := exceedsResourceLimits(
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
func exceedsResourceLimits(used, total schedulerobjects.ResourceList, limits map[string]float64) (bool, string) {
	for resourceType, limit := range limits {
		totalAmount := total.Get(resourceType)
		usedAmount := used.Get(resourceType)
		if armadaresource.QuantityAsFloat64(usedAmount)/armadaresource.QuantityAsFloat64(totalAmount) > limit {
			return true, fmt.Sprintf("scheduling would exceed %s quota", resourceType)
		}
	}
	return false, ""
}
