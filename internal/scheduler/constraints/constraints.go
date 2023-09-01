package constraints

import (
	"fmt"
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
	UnschedulableReasonGlobalRateLimitExceeded          = "global scheduling rate limit exceeded"
	UnschedulableReasonQueueRateLimitExceeded           = "queue scheduling rate limit exceeded"
)

// IsTerminalUnschedulableReason returns true if reason indicates
// it's not possible to schedule any more jobs in this round.
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
	if reason == UnschedulableReasonGlobalRateLimitExceeded {
		return true
	}
	return false
}

// IsTerminalQueueUnschedulableReason returns true if reason indicates
// it's not possible to schedule any more jobs from this queue in this round.
func IsTerminalQueueUnschedulableReason(reason string) bool {
	return reason == UnschedulableReasonQueueRateLimitExceeded
}

// SchedulingConstraints contains scheduling constraints, e.g., per-queue resource limits.
type SchedulingConstraints struct {
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
	// Limits total resources allocated to jobs of this priority class per queue.
	MaximumResourcesPerQueue schedulerobjects.ResourceList
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
			PriorityClassName:        name,
			PriorityClassPriority:    priorityClass.Priority,
			MaximumResourcesPerQueue: absoluteFromRelativeLimits(totalResources, maximumResourceFractionPerQueue),
		}
	}
	maximumResourceFractionToSchedule := config.MaximumResourceFractionToSchedule
	if m, ok := config.MaximumResourceFractionToScheduleByPool[pool]; ok {
		// Use pool-specific config is available.
		maximumResourceFractionToSchedule = m
	}
	return SchedulingConstraints{
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

func (constraints *SchedulingConstraints) CheckRoundConstraints(sctx *schedulercontext.SchedulingContext, queue string) (bool, string, error) {
	// MaximumResourcesToSchedule check.
	if !sctx.ScheduledResources.IsStrictlyLessOrEqual(constraints.MaximumResourcesToSchedule) {
		return false, UnschedulableReasonMaximumResourcesScheduled, nil
	}

	// Global rate limiter check.
	fmt.Println("Global tokens", sctx.Limiter.TokensAt(sctx.Started))
	if sctx.Limiter != nil && sctx.Limiter.TokensAt(sctx.Started) <= 0 {
		return false, UnschedulableReasonGlobalRateLimitExceeded, nil
	}
	// fmt.Println("Global tokens again", sctx.Limiter.TokensAt(sctx.Started))

	// Per-queue rate limiter check.
	fmt.Println("Per-queue tokens", sctx.QueueSchedulingContexts[queue].Limiter.TokensAt(sctx.Started))
	if qctx := sctx.QueueSchedulingContexts[queue]; qctx != nil && qctx.Limiter != nil && qctx.Limiter.TokensAt(sctx.Started) <= 0 {
		return false, UnschedulableReasonQueueRateLimitExceeded, nil
	}
	// fmt.Println("Per-queue tokens again", sctx.QueueSchedulingContexts[queue].Limiter.TokensAt(sctx.Started))

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
		if !qctx.AllocatedByPriorityClass[priorityClassName].IsStrictlyLessOrEqual(priorityClassConstraint.MaximumResourcesPerQueue) {
			return false, UnschedulableReasonMaximumResourcesPerQueueExceeded, nil
		}
	}
	return true, "", nil
}

// ScaleQuantity scales q in-place by a factor f.
// This functions overflows for quantities the milli value of which can't be expressed as an int64.
// E.g., 1Pi is ok, but not 10Pi.
func ScaleQuantity(q resource.Quantity, f float64) resource.Quantity {
	q.SetMilli(int64(math.Round(float64(q.MilliValue()) * f)))
	return q
}
