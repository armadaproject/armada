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
	// Indicates that the limit on resources scheduled per round has been exceeded.
	MaximumResourcesScheduledUnschedulableReason = "maximum resources scheduled"

	// Indicates that a queue has been assigned more than its allowed amount of resources.
	MaximumResourcesPerQueueExceededUnschedulableReason = "maximum total resources for this queue exceeded"

	// Indicates that the scheduling rate limit has been exceeded.
	GlobalRateLimitExceededUnschedulableReason = "global scheduling rate limit exceeded"
	QueueRateLimitExceededUnschedulableReason  = "queue scheduling rate limit exceeded"

	// Indicates that scheduling a gang would exceed the rate limit.
	GlobalRateLimitExceededByGangUnschedulableReason = "gang would exceed global scheduling rate limit"
	QueueRateLimitExceededByGangUnschedulableReason  = "gang would exceed queue scheduling rate limit"

	// Indicates that the number of jobs in a gang exceeds the burst size.
	// This means the gang can not be scheduled without first increasing the burst size.
	GangExceedsGlobalBurstSizeUnschedulableReason = "gang cardinality too large: exceeds global max burst size"
	GangExceedsQueueBurstSizeUnschedulableReason  = "gang cardinality too large: exceeds queue max burst size"
)

// IsTerminalUnschedulableReason returns true if reason indicates
// it's not possible to schedule any more jobs in this round.
func IsTerminalUnschedulableReason(reason string) bool {
	if reason == MaximumResourcesScheduledUnschedulableReason {
		return true
	}
	if reason == GlobalRateLimitExceededUnschedulableReason {
		return true
	}
	return false
}

// IsTerminalQueueUnschedulableReason returns true if reason indicates
// it's not possible to schedule any more jobs from this queue in this round.
func IsTerminalQueueUnschedulableReason(reason string) bool {
	return reason == QueueRateLimitExceededUnschedulableReason
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

// ScaleQuantity scales q in-place by a factor f.
// This functions overflows for quantities the milli value of which can't be expressed as an int64.
// E.g., 1Pi is ok, but not 10Pi.
func ScaleQuantity(q resource.Quantity, f float64) resource.Quantity {
	q.SetMilli(int64(math.Round(float64(q.MilliValue()) * f)))
	return q
}

func (constraints *SchedulingConstraints) CheckRoundConstraints(sctx *schedulercontext.SchedulingContext) (bool, string, error) {
	// MaximumResourcesToSchedule check.
	if !sctx.ScheduledResources.IsStrictlyLessOrEqual(constraints.MaximumResourcesToSchedule) {
		return false, MaximumResourcesScheduledUnschedulableReason, nil
	}
	return true, "", nil
}

func (constraints *SchedulingConstraints) CheckConstraints(
	sctx *schedulercontext.SchedulingContext,
	gctx *schedulercontext.GangSchedulingContext,
) (bool, string, error) {
	qctx := sctx.QueueSchedulingContexts[gctx.Queue]
	if qctx == nil {
		return false, "", errors.Errorf("no QueueSchedulingContext for queue %s", gctx.Queue)
	}

	// Check that the job is large enough for this executor.
	if ok, unschedulableReason := RequestsAreLargeEnough(gctx.TotalResourceRequests, constraints.MinimumJobSize); !ok {
		return false, unschedulableReason, nil
	}

	// Global rate limiter check.
	tokens := sctx.Limiter.TokensAt(sctx.Started)
	if tokens <= 0 {
		return false, GlobalRateLimitExceededUnschedulableReason, nil
	}
	if sctx.Limiter.Burst() < gctx.Cardinality() {
		return false, GangExceedsGlobalBurstSizeUnschedulableReason, nil
	}
	if tokens < float64(gctx.Cardinality()) {
		return false, GlobalRateLimitExceededByGangUnschedulableReason, nil
	}

	// Per-queue rate limiter check.
	tokens = qctx.Limiter.TokensAt(sctx.Started)
	if tokens <= 0 {
		return false, QueueRateLimitExceededUnschedulableReason, nil
	}
	if qctx.Limiter.Burst() < gctx.Cardinality() {
		return false, GangExceedsQueueBurstSizeUnschedulableReason, nil
	}
	if tokens < float64(gctx.Cardinality()) {
		return false, QueueRateLimitExceededByGangUnschedulableReason, nil
	}

	// PriorityClassSchedulingConstraintsByPriorityClassName check.
	if priorityClassConstraint, ok := constraints.PriorityClassSchedulingConstraintsByPriorityClassName[gctx.PriorityClassName]; ok {
		if !qctx.AllocatedByPriorityClass[gctx.PriorityClassName].IsStrictlyLessOrEqual(priorityClassConstraint.MaximumResourcesPerQueue) {
			return false, MaximumResourcesPerQueueExceededUnschedulableReason, nil
		}
	}
	return true, "", nil
}

func RequestsAreLargeEnough(totalResourceRequests, minRequest schedulerobjects.ResourceList) (bool, string) {
	for t, minQuantity := range minRequest.Resources {
		q := totalResourceRequests.Get(t)
		if minQuantity.Cmp(q) == 1 {
			return false, fmt.Sprintf("job requests %s %s, but the minimum is %s", q.String(), t, minQuantity.String())
		}
	}
	return true, ""
}
