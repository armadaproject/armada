package constraints

import (
	"math"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

const (
	UnschedulableReasonMaximumResourcesScheduled        = "maximum resources scheduled"
	UnschedulableReasonMaximumNumberOfJobsScheduled     = "maximum number of jobs scheduled"
	UnschedulableReasonMaximumNumberOfGangsScheduled    = "maximum number of gangs scheduled"
	UnschedulableReasonMaximumResourcesPerQueueExceeded = "maximum total resources for this queue exceeded"
)

// IsTerminalUnschedulableReason returns true if reason indicates it's not possible to schedule any more jobs in this round.
func IsTerminalUnschedulableReason(reason string) bool {
	if reason == UnschedulableReasonMaximumResourcesScheduled {
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
	// Limits total resources scheduled per invocation.
	MaximumResourcesToSchedule schedulerobjects.ResourceList
}

// PriorityClassSchedulingConstraints contains scheduling constraints that apply to jobs of a specific priority class.
type PriorityClassSchedulingConstraints struct {
	PriorityClassName     string
	PriorityClassPriority int32
	// Prevents jobs of this priority class from being scheduled if doing so would exceed
	// cumulative resource usage at priority priorityClassPriority for the queue the job originates from.
	//
	// Cumulative resource usage at priority x includes resources allocated to jobs of priorityClassPriority x or lower.
	MaximumCumulativeResourcesPerQueue schedulerobjects.ResourceList
}

func SchedulingConstraintsFromSchedulingConfig(
	pool string,
	totalResources schedulerobjects.ResourceList,
	minimumJobSize schedulerobjects.ResourceList,
	config configuration.SchedulingConfig,
) SchedulingConstraints {
	priorityClassSchedulingConstraintsByPriorityClassName := make(map[string]PriorityClassSchedulingConstraints, len(config.Preemption.PriorityClasses))
	for name, priorityClass := range config.Preemption.PriorityClasses {
		maximumResourceFractionPerQueue := priorityClass.MaximumResourceFractionPerQueue
		if m, ok := priorityClass.MaximumResourceFractionPerQueueByPool[pool]; ok {
			// Use pool-specific config is available.
			maximumResourceFractionPerQueue = m
		}
		priorityClassSchedulingConstraintsByPriorityClassName[name] = PriorityClassSchedulingConstraints{
			PriorityClassName:                  name,
			PriorityClassPriority:              priorityClass.Priority,
			MaximumCumulativeResourcesPerQueue: absoluteFromRelativeLimits(totalResources, maximumResourceFractionPerQueue),
		}
	}
	maximumResourceFractionToSchedule := config.MaximumResourceFractionToSchedule
	if m, ok := config.MaximumResourceFractionToScheduleByPool[pool]; ok {
		// Use pool-specific config is available.
		maximumResourceFractionToSchedule = m
	}
	return SchedulingConstraints{
		MaximumJobsToSchedule:      config.MaximumJobsToSchedule,
		MaximumGangsToSchedule:     config.MaximumGangsToSchedule,
		MaxQueueLookback:           config.MaxQueueLookback,
		MinimumJobSize:             minimumJobSize,
		MaximumResourcesToSchedule: absoluteFromRelativeLimits(totalResources, maximumResourceFractionToSchedule),
		PriorityClassSchedulingConstraintsByPriorityClassName: priorityClassSchedulingConstraintsByPriorityClassName,
	}
}

func absoluteFromRelativeLimits(totalResources schedulerobjects.ResourceList, relativeLimits map[string]float64) schedulerobjects.ResourceList {
	absoluteLimits := schedulerobjects.NewResourceList(len(relativeLimits))
	for t, f := range relativeLimits {
		absoluteLimits.Set(t, ScaleQuantity(totalResources.Get(t).DeepCopy(), f))
	}
	return absoluteLimits
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

	// MaximumResourcesToSchedule check.
	if exceedsResourceLimits(sctx.ScheduledResources, constraints.MaximumResourcesToSchedule) {
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
		if exceedsResourceLimits(
			// TODO: Avoid allocation.
			schedulerobjects.QuantityByPriorityAndResourceType(allocatedByPriorityAndResourceType).AggregateByResource(),
			priorityClassConstraint.MaximumCumulativeResourcesPerQueue,
		) {
			return false, UnschedulableReasonMaximumResourcesPerQueueExceeded, nil
		}
	}
	return true, "", nil
}

// exceedsResourceLimits returns true if used/total > limits for some resource.
func exceedsResourceLimits(used, limits schedulerobjects.ResourceList) bool {
	return !used.IsStrictlyLessOrEqual(limits)
}

// ScaleQuantity scales q in-place by a factor f.
// This functions overflows for quantities the milli value of which can't be expressed as an int64.
// E.g., 1Pi is ok, but not 10Pi.
func ScaleQuantity(q resource.Quantity, f float64) resource.Quantity {
	q.SetMilli(int64(math.Round(float64(q.MilliValue()) * f)))
	return q
}
