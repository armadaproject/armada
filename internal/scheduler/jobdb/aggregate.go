package jobdb

import (
	"github.com/benbjohnson/immutable"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

// QueuedDemand maintains an incrementally updated aggregate of queued demand by
// pool, then queue, then priority class, so the scheduler can read queued demand
// without scanning every queued job.
type QueuedDemand struct {
	byPool *immutable.Map[string, *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]]]
}

func NewQueuedDemand() *QueuedDemand {
	return &QueuedDemand{
		byPool: immutable.NewMap[string, *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]]](nil),
	}
}

// Clone returns a copy of the aggregate for use by a write transaction. The copy
// shares the current state; changes to it are not visible until committed.
func (a *QueuedDemand) Clone() *QueuedDemand {
	if a == nil {
		return NewQueuedDemand()
	}
	return &QueuedDemand{
		byPool: a.byPool,
	}
}

// add incorporates job into the aggregate. Jobs that are not queued or are in a
// terminal state are ignored.
func (a *QueuedDemand) add(job *Job) {
	if a == nil || job == nil || job.InTerminalState() || !job.Queued() {
		return
	}

	req := job.AllResourceRequirements()
	queue := job.Queue()
	pc := job.PriorityClassName()
	forEachDistinctPool(job.pools, func(pool string) {
		poolMap, _ := a.byPool.Get(pool)
		if poolMap == nil {
			poolMap = immutable.NewMap[string, *immutable.Map[string, internaltypes.ResourceList]](nil)
		}
		queueMap, _ := poolMap.Get(queue)
		if queueMap == nil {
			queueMap = immutable.NewMap[string, internaltypes.ResourceList](nil)
		}
		current, _ := queueMap.Get(pc)
		a.byPool = a.byPool.Set(pool, poolMap.Set(queue, queueMap.Set(pc, current.Add(req))))
	})
}

// remove removes job from the aggregate. The job must be in the same state as
// when it was added; any inconsistency is reported as an invariant violation.
func (a *QueuedDemand) remove(job *Job) {
	if a == nil || job == nil || job.InTerminalState() || !job.Queued() {
		return
	}

	req := job.AllResourceRequirements()
	queue := job.Queue()
	pc := job.PriorityClassName()
	forEachDistinctPool(job.pools, func(pool string) {
		poolMap, ok := a.byPool.Get(pool)
		if !ok || poolMap == nil {
			recordAggregateInvariantViolation("remove_missing_pool")
			return
		}
		queueMap, ok := poolMap.Get(queue)
		if !ok || queueMap == nil {
			recordAggregateInvariantViolation("remove_missing_queue")
			return
		}
		current, ok := queueMap.Get(pc)
		if !ok {
			recordAggregateInvariantViolation("remove_missing_priority_class")
			return
		}
		remaining := current.Subtract(req)
		if remaining.HasNegativeValues() {
			recordAggregateInvariantViolation("remove_negative_remaining")
			return
		}
		if remaining.AllZero() {
			queueMap = queueMap.Delete(pc)
		} else {
			queueMap = queueMap.Set(pc, remaining)
		}
		if queueMap.Len() == 0 {
			poolMap = poolMap.Delete(queue)
		} else {
			poolMap = poolMap.Set(queue, queueMap)
		}
		if poolMap.Len() == 0 {
			a.byPool = a.byPool.Delete(pool)
		} else {
			a.byPool = a.byPool.Set(pool, poolMap)
		}
	})
}

// getQueuedDemand returns queued demand for currentPool by queue and priority
// class. Unknown and cordoned queues are excluded.
func (a *QueuedDemand) getQueuedDemand(
	currentPool string,
	knownQueues map[string]bool,
	cordonedQueues map[string]bool,
) map[string]map[string]internaltypes.ResourceList {
	demand := map[string]map[string]internaltypes.ResourceList{}
	if a == nil || a.byPool == nil {
		return demand
	}
	poolMap, ok := a.byPool.Get(currentPool)
	if !ok || poolMap == nil {
		return demand
	}
	poolIt := poolMap.Iterator()
	for !poolIt.Done() {
		queue, queueMap, _ := poolIt.Next()
		if queueMap == nil || cordonedQueues[queue] || !queueKnown(knownQueues, queue) {
			continue
		}
		byPriorityClass, ok := demand[queue]
		if !ok {
			byPriorityClass = map[string]internaltypes.ResourceList{}
			demand[queue] = byPriorityClass
		}
		queueIt := queueMap.Iterator()
		for !queueIt.Done() {
			pc, rl, _ := queueIt.Next()
			byPriorityClass[pc] = byPriorityClass[pc].Add(rl)
		}
	}
	return demand
}

// forEachDistinctPool calls f once per distinct pool in pools.
func forEachDistinctPool(pools []string, f func(pool string)) {
	for i, pool := range pools {
		duplicate := false
		for _, previous := range pools[:i] {
			if previous == pool {
				duplicate = true
				break
			}
		}
		if !duplicate {
			f(pool)
		}
	}
}

func queueKnown(knownQueues map[string]bool, queue string) bool {
	if knownQueues == nil {
		return true
	}
	return knownQueues[queue]
}
