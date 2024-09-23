package constraints

import (
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/pkg/api"
)

// SchedulingConstraints contains scheduling constraints, e.g. per-queue resource limits.
type SchedulingConstraints interface {
	CheckRoundConstraints(sctx *context.SchedulingContext) (bool, string, error)
	CheckJobConstraints(sctx *context.SchedulingContext, gctx *context.GangSchedulingContext) (bool, string, error)
	CapResources(queue string, resourcesByPc map[string]internaltypes.ResourceList) map[string]internaltypes.ResourceList
}

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
	priorityClasses map[string]types.PriorityClass,
	totalResources internaltypes.ResourceList,
	maximumResourceFractionToSchedule float64,
	queues []*api.Queue,
	cordonStatusByQueue map[string]bool) SchedulingConstraints {
	return &schedulingConstraints{
		cordonedQueues:                         cordonStatusByQueue,
		maximumResourcesToSchedule:             totalResources.Scale(maximumResourceFractionToSchedule),
		resourceLimitsPerQueuePerPriorityClass: calculatePerQueueLimits(totalResources, pool, priorityClasses, queues),
	}
}

func (c *schedulingConstraints) CheckRoundConstraints(sctx *context.SchedulingContext) (bool, string, error) {
	scheduledResources := internaltypes.ResourceList{} // TODO should be sctx.ScheduledResources
	if scheduledResources.StrictlyLessOrEqualThan(c.maximumResourcesToSchedule) {
		return true, "", nil
	}
	return false, MaximumResourcesScheduled, nil
}

func (c *schedulingConstraints) CheckJobConstraints(sctx *context.SchedulingContext, gctx *context.GangSchedulingContext) (bool, string, error) {

	// Queue Context can be missing if we have a job in the system where the queue has been deleted
	// TODO: We should ensure such jobs are filtered out well before this point!
	qctx, ok := sctx.QueueSchedulingContexts[gctx.Queue]
	if !ok {
		return false, "", errors.Errorf("no QueueSchedulingContext for queue %s", gctx.Queue)
	}

	// Queue cordoned
	if c.cordonedQueues[qctx.Queue] {
		return false, QueueCordoned, nil
	}

	// Global rate limiter check.
	tokens := sctx.Limiter.TokensAt(sctx.Started)
	if tokens < float64(gctx.Cardinality()) || sctx.Limiter.Burst() < gctx.Cardinality() {
		return false, GlobalRateLimitExceeded, nil
	}

	// Per-queue rate limiter check.
	tokens = qctx.Limiter.TokensAt(sctx.Started)
	if tokens < float64(gctx.Cardinality()) || qctx.Limiter.Burst() < gctx.Cardinality() {
		return false, QueueRateLimitExceeded, nil
	}

	// Resource Allocation check
	queueLimit, haslimit := c.resourceLimitsPerQueuePerPriorityClass[qctx.Queue][gctx.PriorityClassName]
	var allocatedResources internaltypes.ResourceList = qctx.AllocatedByPriorityClass[gctx.PriorityClassName].Resources
	if haslimit && !allocatedResources.StrictlyLessOrEqualThan(queueLimit) {
		return false, MaximumResourcesExceeded, nil
	}

	return true, "", nil
}

func (c *schedulingConstraints) CapResources(queue string, resourcesByPc map[string]internaltypes.ResourceList) map[string]internaltypes.ResourceList {
	perQueueLimit, ok := c.resourceLimitsPerQueuePerPriorityClass[queue]
	if !ok {
		return resourcesByPc
	}
	cappedResources := make(map[string]internaltypes.ResourceList, len(resourcesByPc))
	for pc, resources := range resourcesByPc {
		limits, ok := perQueueLimit[pc]
		if !ok {
			cappedResources[pc] = resources
		} else {
			resources.Cap(limits)
		}
	}
	return cappedResources
}

func calculatePerQueueLimits(totalResources internaltypes.ResourceList, pool string, priorityClasses map[string]types.PriorityClass, queues []*api.Queue) map[string]map[string]internaltypes.ResourceList {

	// First we work out the default limit per pool
	defaultScalingFactorsByPc := map[string]map[string]float64{}
	defaultResourceLimitsByPc := map[string]internaltypes.ResourceList{}
	for pcName, pc := range priorityClasses {
		defaultLimit := pc.MaximumResourceFractionPerQueue
		poolLimit := pc.MaximumResourceFractionPerQueueByPool[pool]
		defaultScalingFactors := util.MergeMaps(defaultLimit, poolLimit)
		defaultScalingFactorsByPc[pcName] = defaultScalingFactors
		defaultResourceLimitsByPc[pcName] = totalResources.ScaleCustom(defaultScalingFactors)
	}

	limitsPerQueuePerPc := make(map[string]map[string]internaltypes.ResourceList, len(queues))

	// Then we go apply any queue-level overrides
	for _, queue := range queues {
		// There are no queue-specific limits
		if len(queue.ResourceLimitsByPriorityClassName) == 0 {
			limitsPerQueuePerPc[queue.Name] = defaultResourceLimitsByPc
		} else {
			for pc, _ := range priorityClasses {
				queueLimits, ok := queue.ResourceLimitsByPriorityClassName[pc]
				if ok {
					defaultFraction := defaultScalingFactorsByPc[pc]
					fractionLimit := util.MergeMaps(defaultFraction, queueLimits.MaximumResourceFraction)
					fractionLimit = util.MergeMaps(fractionLimit, queueLimits.MaximumResourceFractionByPool[pool].GetMaximumResourceFraction())
					limitsPerQueuePerPc[queue.Name][pc] = defaultResourceLimitsByPc[pc].ScaleCustom(fractionLimit)
				} else {
					limitsPerQueuePerPc[queue.Name][pc] = defaultResourceLimitsByPc[pc]
				}
			}
		}
	}
	return limitsPerQueuePerPc
}
