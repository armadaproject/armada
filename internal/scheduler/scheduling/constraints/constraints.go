package constraints

import (
	"github.com/pkg/errors"

	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/pkg/api"
)

// SchedulingConstraints contains scheduling constraints, e.g. per-queue resource limits.
type SchedulingConstraints interface {
	CheckRoundConstraints(sctx *context.SchedulingContext) (bool, string, error)
	CheckJobConstraints(sctx *context.SchedulingContext, gctx *context.GangSchedulingContext) (bool, string, error)
	CapResources(queue string, resourcesByPc map[string]internaltypes.ResourceList) map[string]internaltypes.ResourceList
}

const (
	// Indicates that the limit on resources scheduled per round has been exceeded.
	MaximumResourcesScheduledUnschedulableReason = "maximum resources scheduled"

	// Indicates that a queue has been assigned more than its allowed amount of resources.
	MaximumResourcesPerQueueExceededUnschedulableReason = "maximum total resources for this queue exceeded"

	// Indicates that the scheduling rate limit has been exceeded.
	GlobalRateLimitExceededUnschedulableReason = "global scheduling rate limit exceeded"
	QueueRateLimitExceededUnschedulableReason  = "queue scheduling rate limit exceeded"
	QueueCordonedUnschedulableReason           = "queue cordoned"

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
	return reason == QueueRateLimitExceededUnschedulableReason || reason == QueueCordonedUnschedulableReason
}

// SchedulingConstraints contains scheduling constraints, e.g., per-queue resource limits.
type schedulingConstraints struct {
	// Limits total resources scheduled per scheduling round.
	maximumResourcesToSchedule internaltypes.ResourceList
	// Queues that are cordoned (i.e. no jobs may be scheduled on them)
	cordonedQueues map[string]bool
	// Resource limits by queue and priority class. E.g. "Queue A is limited to 100 cpu at priority class armada-default"
	resourceLimitsPerQueuePerPriorityClass map[string]map[string]internaltypes.ResourceList
}

func NewSchedulingConstraints(
	pool string,
	totalResources internaltypes.ResourceList,
	config configuration.SchedulingConfig,
	queues []*api.Queue,
	rlFactory *internaltypes.ResourceListFactory) SchedulingConstraints {

	cordonedQueues := armadamaps.FromSlice(queues,
		func(q *api.Queue) string { return q.Name },
		func(q *api.Queue) bool { return q.Cordoned })

	return &schedulingConstraints{
		cordonedQueues:                         cordonedQueues,
		maximumResourcesToSchedule:             calculatePerRoundLimits(totalResources, pool, config, rlFactory),
		resourceLimitsPerQueuePerPriorityClass: calculatePerQueueLimits(totalResources, pool, config.PriorityClasses, queues, rlFactory),
	}
}

func (constraints *schedulingConstraints) CheckRoundConstraints(sctx *context.SchedulingContext) (bool, string, error) {
	// maximumResourcesToSchedule check.
	if sctx.ScheduledResources.Exceeds(constraints.maximumResourcesToSchedule) {
		return false, MaximumResourcesScheduledUnschedulableReason, nil
	}
	return true, "", nil
}

func (constraints *schedulingConstraints) CheckJobConstraints(
	sctx *context.SchedulingContext,
	gctx *context.GangSchedulingContext,
) (bool, string, error) {
	qctx := sctx.QueueSchedulingContexts[gctx.Queue]
	if qctx == nil {
		return false, "", errors.Errorf("no QueueSchedulingContext for queue %s", gctx.Queue)
	}

	// Queue cordoned
	if constraints.cordonedQueues[qctx.Queue] {
		return false, QueueCordonedUnschedulableReason, nil
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

	// Quantity scheduled by queue and priority class
	queueLimit, haslimit := constraints.resourceLimitsPerQueuePerPriorityClass[qctx.Queue][gctx.PriorityClassName]
	allocatedResources := qctx.AllocatedByPriorityClass[gctx.PriorityClassName]
	if haslimit && allocatedResources.Exceeds(queueLimit) {
		return false, UnschedulableReasonMaximumResourcesExceeded, nil
	}

	return true, "", nil
}

func (c *schedulingConstraints) CapResources(queue string, resourcesByPc map[string]internaltypes.ResourceList) map[string]internaltypes.ResourceList {
	perQueueLimit, ok := c.resourceLimitsPerQueuePerPriorityClass[queue]
	if !ok {
		return resourcesByPc
	}
	cappedResourcesByPc := make(map[string]internaltypes.ResourceList, len(resourcesByPc))
	for pc, resources := range resourcesByPc {
		cappedResourcesByPc[pc] = resources.Cap(perQueueLimit[pc])
	}
	return cappedResourcesByPc
}

func calculatePerRoundLimits(
	totalResources internaltypes.ResourceList,
	pool string,
	config configuration.SchedulingConfig,
	rlFactory *internaltypes.ResourceListFactory,
) internaltypes.ResourceList {
	maximumResourceFractionToSchedule := config.MaximumResourceFractionToSchedule
	if m, ok := config.MaximumResourceFractionToScheduleByPool[pool]; ok {
		// Use pool-specific config is available.
		// Should do util.MergeMaps really but don't want to change existing behaviour.
		maximumResourceFractionToSchedule = m
	}
	return totalResources.Multiply(rlFactory.MakeResourceFractionList(maximumResourceFractionToSchedule, 1.0))
}

func calculatePerQueueLimits(totalResources internaltypes.ResourceList,
	pool string,
	priorityClasses map[string]types.PriorityClass,
	queues []*api.Queue,
	rlFactory *internaltypes.ResourceListFactory,
) map[string]map[string]internaltypes.ResourceList {

	// First we work out the default limit per pool
	defaultScalingFactorsByPc := map[string]map[string]float64{}
	for pcName, pc := range priorityClasses {
		defaultLimit := pc.MaximumResourceFractionPerQueue
		poolLimit := pc.MaximumResourceFractionPerQueueByPool[pool]
		defaultScalingFactorsByPc[pcName] = util.MergeMaps(defaultLimit, poolLimit)
	}

	limitsPerQueuePerPc := make(map[string]map[string]internaltypes.ResourceList, len(queues))

	// Then we go apply any queue-level overrides
	for _, queue := range queues {
		limitsPerQueuePerPc[queue.Name] = map[string]internaltypes.ResourceList{}
		// There are no queue-specific limits
		for pc, _ := range priorityClasses {
			fractions := defaultScalingFactorsByPc[pc]
			queueLimits, ok := queue.ResourceLimitsByPriorityClassName[pc]
			if ok {
				fractions = util.MergeMaps(fractions, queueLimits.MaximumResourceFraction)
				queuePoolConfig, ok := queueLimits.MaximumResourceFractionByPool[pool]
				if ok {
					fractions = util.MergeMaps(fractions, queuePoolConfig.GetMaximumResourceFraction())
				}
			}
			limitsPerQueuePerPc[queue.Name][pc] = totalResources.Multiply(rlFactory.MakeResourceFractionList(fractions, 1.0))
		}
	}
	return limitsPerQueuePerPc
}
