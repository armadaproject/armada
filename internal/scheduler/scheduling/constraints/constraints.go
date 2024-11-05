package constraints

import (
	"math"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/pkg/api"
)

const (
	// Indicates that the limit on resources scheduled per round has been exceeded.
	MaximumResourcesScheduledUnschedulableReason = "maximum resources scheduled"

	// Indicates that a queue has been assigned more than its allowed amount of resources.
	MaximumResourcesPerQueueExceededUnschedulableReason = "maximum total resources for this queue exceeded"

	// Indicates that the scheduling rate limit has been exceeded.
	GlobalRateLimitExceededUnschedulableReason = "global scheduling rate limit exceeded"
	QueueRateLimitExceededUnschedulableReason  = "queue scheduling rate limit exceeded"
	SchedulingPausedOnQueueUnschedulableReason = "scheduling paused on queue"

	// Indicates that scheduling a gang would exceed the rate limit.
	GlobalRateLimitExceededByGangUnschedulableReason = "gang would exceed global scheduling rate limit"
	QueueRateLimitExceededByGangUnschedulableReason  = "gang would exceed queue scheduling rate limit"

	// Indicates that the number of jobs in a gang exceeds the burst size.
	// This means the gang can not be scheduled without first increasing the burst size.
	GangExceedsGlobalBurstSizeUnschedulableReason = "gang cardinality too large: exceeds global max burst size"
	GangExceedsQueueBurstSizeUnschedulableReason  = "gang cardinality too large: exceeds queue max burst size"

	// Indicates that jobs cannot be scheduled due current executor state
	GangDoesNotFitUnschedulableReason = "unable to schedule gang since minimum cardinality not met"
	JobDoesNotFitUnschedulableReason  = "job does not fit on any node"

	UnschedulableReasonMaximumResourcesExceeded = "resource limit exceeded"
)

func UnschedulableReasonIsPropertyOfGang(reason string) bool {
	return reason == GangExceedsGlobalBurstSizeUnschedulableReason || reason == JobDoesNotFitUnschedulableReason || reason == GangDoesNotFitUnschedulableReason
}

// IsTerminalUnschedulableReason returns true if reason indicates
// it's not possible to schedule any more jobs in this round.
func IsTerminalUnschedulableReason(reason string) bool {
	return reason == MaximumResourcesScheduledUnschedulableReason ||
		reason == GlobalRateLimitExceededUnschedulableReason
}

// IsTerminalQueueUnschedulableReason returns true if reason indicates
// it's not possible to schedule any more jobs from this queue in this round.
func IsTerminalQueueUnschedulableReason(reason string) bool {
	return reason == QueueRateLimitExceededUnschedulableReason || reason == SchedulingPausedOnQueueUnschedulableReason
}

// SchedulingConstraints contains scheduling constraints, e.g., per-queue resource limits.
type SchedulingConstraints struct {
	// Scheduling constraints by priority class.
	priorityClassSchedulingConstraintsByPriorityClassName map[string]priorityClassSchedulingConstraints
	// Scheduling constraints for specific queues.
	// If present for a particular queue, global limits (i.e., priorityClassSchedulingConstraintsByPriorityClassName)
	// do not apply for that queue.
	queueSchedulingConstraintsByQueueName map[string]queueSchedulingConstraints
	// Limits total resources scheduled per invocation.
	maximumResourcesToSchedule map[string]resource.Quantity
}

// queueSchedulingConstraints contains per-queue scheduling constraints.
type queueSchedulingConstraints struct {
	// Scheduling constraints by priority class.
	PriorityClassSchedulingConstraintsByPriorityClassName map[string]priorityClassSchedulingConstraints
	// Determines whether scheduling has been paused for this queue
	Cordoned bool
}

// priorityClassSchedulingConstraints contains scheduling constraints that apply to jobs of a specific priority class.
type priorityClassSchedulingConstraints struct {
	PriorityClassName string
	// Limits total resources allocated to jobs of this priority class per queue.
	MaximumResourcesPerQueue map[string]resource.Quantity
}

func NewSchedulingConstraints(pool string, totalResources schedulerobjects.ResourceList, config configuration.SchedulingConfig, queues []*api.Queue) SchedulingConstraints {
	priorityClassSchedulingConstraintsByPriorityClassName := make(map[string]priorityClassSchedulingConstraints, len(config.PriorityClasses))
	for name, priorityClass := range config.PriorityClasses {
		maximumResourceFractionPerQueue := priorityClass.MaximumResourceFractionPerQueue
		if m, ok := priorityClass.MaximumResourceFractionPerQueueByPool[pool]; ok {
			// Use pool-specific config is available.
			maximumResourceFractionPerQueue = util.MergeMaps(maximumResourceFractionPerQueue, m)
		}
		priorityClassSchedulingConstraintsByPriorityClassName[name] = priorityClassSchedulingConstraints{
			PriorityClassName:        name,
			MaximumResourcesPerQueue: absoluteFromRelativeLimits(totalResources.Resources, maximumResourceFractionPerQueue),
		}
	}

	queueSchedulingConstraintsByQueueName := make(map[string]queueSchedulingConstraints, len(queues))
	for _, queue := range queues {
		priorityClassSchedulingConstraintsByPriorityClassNameForQueue := make(map[string]priorityClassSchedulingConstraints, len(queue.ResourceLimitsByPriorityClassName))
		for priorityClassName, priorityClassResourceLimits := range queue.ResourceLimitsByPriorityClassName {
			maximumResourceFraction := priorityClassResourceLimits.MaximumResourceFraction
			if m, ok := priorityClassResourceLimits.MaximumResourceFractionByPool[pool]; ok {
				// Use pool-specific maximum resource fraction if available.
				maximumResourceFraction = util.MergeMaps(maximumResourceFraction, m.MaximumResourceFraction)
			}
			priorityClassSchedulingConstraintsByPriorityClassNameForQueue[priorityClassName] = priorityClassSchedulingConstraints{
				PriorityClassName:        priorityClassName,
				MaximumResourcesPerQueue: absoluteFromRelativeLimits(totalResources.Resources, maximumResourceFraction),
			}
		}
		queueSchedulingConstraintsByQueueName[queue.Name] = queueSchedulingConstraints{
			PriorityClassSchedulingConstraintsByPriorityClassName: priorityClassSchedulingConstraintsByPriorityClassNameForQueue,
			Cordoned: queue.Cordoned,
		}
	}

	maximumResourceFractionToSchedule := config.MaximumResourceFractionToSchedule
	if m, ok := config.MaximumResourceFractionToScheduleByPool[pool]; ok {
		// Use pool-specific config is available.
		maximumResourceFractionToSchedule = m
	}
	return SchedulingConstraints{
		maximumResourcesToSchedule:                            absoluteFromRelativeLimits(totalResources.Resources, maximumResourceFractionToSchedule),
		priorityClassSchedulingConstraintsByPriorityClassName: priorityClassSchedulingConstraintsByPriorityClassName,
		queueSchedulingConstraintsByQueueName:                 queueSchedulingConstraintsByQueueName,
	}
}

func absoluteFromRelativeLimits(totalResources map[string]resource.Quantity, relativeLimits map[string]float64) map[string]resource.Quantity {
	absoluteLimits := make(map[string]resource.Quantity, len(relativeLimits))
	for t, f := range relativeLimits {
		absoluteLimits[t] = ScaleQuantity(totalResources[t].DeepCopy(), f)
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

func (constraints *SchedulingConstraints) CheckRoundConstraints(sctx *context.SchedulingContext) (bool, string, error) {
	// maximumResourcesToSchedule check.
	if !isStrictlyLessOrEqual(sctx.ScheduledResources.Resources, constraints.maximumResourcesToSchedule) {
		return false, MaximumResourcesScheduledUnschedulableReason, nil
	}
	return true, "", nil
}

func (constraints *SchedulingConstraints) CheckConstraints(
	sctx *context.SchedulingContext,
	gctx *context.GangSchedulingContext,
) (bool, string, error) {
	qctx := sctx.QueueSchedulingContexts[gctx.Queue]
	if qctx == nil {
		return false, "", errors.Errorf("no QueueSchedulingContext for queue %s", gctx.Queue)
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

	if queueConstraints, ok := constraints.queueSchedulingConstraintsByQueueName[qctx.Queue]; ok && queueConstraints.Cordoned {
		return false, SchedulingPausedOnQueueUnschedulableReason, nil
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

	// queueSchedulingConstraintsByQueueName / priorityClassSchedulingConstraintsByPriorityClassName checks.
	overallResourceLimits := constraints.resolveResourceLimitsForQueueAndPriorityClass(gctx.Queue, gctx.PriorityClassName)
	if !isStrictlyLessOrEqual(qctx.AllocatedByPriorityClass[gctx.PriorityClassName].Resources, overallResourceLimits) {
		return false, UnschedulableReasonMaximumResourcesExceeded, nil
	}

	return true, "", nil
}

func (constraints *SchedulingConstraints) CapResources(queue string, resourcesByPc schedulerobjects.QuantityByTAndResourceType[string]) schedulerobjects.QuantityByTAndResourceType[string] {
	cappedResourcesByPc := schedulerobjects.QuantityByTAndResourceType[string]{}
	for pc, resources := range resourcesByPc {
		overallResourceLimits := constraints.resolveResourceLimitsForQueueAndPriorityClass(queue, pc)
		cappedResources := make(map[string]resource.Quantity, len(resources.Resources))
		for resourceName, qty := range resources.Resources {
			limit, ok := overallResourceLimits[resourceName]
			if ok && qty.Cmp(limit) == 1 {
				cappedResources[resourceName] = limit
			} else {
				cappedResources[resourceName] = qty
			}
		}
		cappedResourcesByPc[pc] = schedulerobjects.ResourceList{Resources: cappedResources}
	}
	return cappedResourcesByPc
}

func (constraints *SchedulingConstraints) resolveResourceLimitsForQueueAndPriorityClass(queue string, priorityClass string) map[string]resource.Quantity {
	queueAndPriorityClassResourceLimits := constraints.getQueueAndPriorityClassResourceLimits(queue, priorityClass)
	priorityClassResourceLimits := constraints.getPriorityClassResourceLimits(priorityClass)
	return util.MergeMaps(priorityClassResourceLimits, queueAndPriorityClassResourceLimits)
}

func (constraints *SchedulingConstraints) getQueueAndPriorityClassResourceLimits(queue string, priorityClass string) map[string]resource.Quantity {
	if queueConstraint, ok := constraints.queueSchedulingConstraintsByQueueName[queue]; ok {
		if priorityClassConstraint, ok := queueConstraint.PriorityClassSchedulingConstraintsByPriorityClassName[priorityClass]; ok {
			return priorityClassConstraint.MaximumResourcesPerQueue
		}
	}
	return map[string]resource.Quantity{}
}

func (constraints *SchedulingConstraints) getPriorityClassResourceLimits(priorityClass string) map[string]resource.Quantity {
	if priorityClassConstraint, ok := constraints.priorityClassSchedulingConstraintsByPriorityClassName[priorityClass]; ok {
		return priorityClassConstraint.MaximumResourcesPerQueue
	}
	return map[string]resource.Quantity{}
}

// isStrictlyLessOrEqual returns false if
// - there is a quantity in b greater than that in a or
// - there is a non-zero quantity in b not in a
// and true otherwise.
func isStrictlyLessOrEqual(a map[string]resource.Quantity, b map[string]resource.Quantity) bool {
	for t, q := range b {
		if q.Cmp(a[t]) == -1 {
			return false
		}
	}
	return true
}
