package jobdb

import (
	"github.com/benbjohnson/immutable"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

// JobAggregate maintains an incrementally updated aggregate of queued demand so
// the scheduler can read queued demand without scanning every queued job.
type JobAggregate struct {
	// Queued demand by pool, then queue, then priority class.
	queuedDemand *immutable.Map[string, *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]]]
	// Number of queued jobs contributing to each bucket, keyed the same way.
	// A bucket exists exactly while its count is positive, so zero-resource
	// jobs keep their (zero-valued) bucket instead of dropping it early.
	queuedCounts *immutable.Map[string, *immutable.Map[string, *immutable.Map[string, int]]]
}

func NewJobAggregate() *JobAggregate {
	return &JobAggregate{
		queuedDemand: immutable.NewMap[string, *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]]](nil),
		queuedCounts: immutable.NewMap[string, *immutable.Map[string, *immutable.Map[string, int]]](nil),
	}
}

// Clone returns a copy of the aggregate for use by a write transaction. The copy
// shares the current state; changes to it are not visible until committed.
func (a *JobAggregate) Clone() *JobAggregate {
	if a == nil {
		return NewJobAggregate()
	}
	return &JobAggregate{
		queuedDemand: a.queuedDemand,
		queuedCounts: a.queuedCounts,
	}
}

// add incorporates job into the aggregate. Jobs that are not queued or are in a
// terminal state are ignored.
func (a *JobAggregate) add(job *Job) {
	if a == nil || job == nil || job.InTerminalState() || !job.Queued() {
		return
	}

	req := job.AllResourceRequirements()
	queue := job.Queue()
	pc := job.PriorityClassName()
	pools := job.pools
	for i, pool := range pools {
		if slices.Contains(pools[:i], pool) {
			continue
		}
		poolMap, _ := a.queuedDemand.Get(pool)
		if poolMap == nil {
			poolMap = immutable.NewMap[string, *immutable.Map[string, internaltypes.ResourceList]](nil)
		}
		queueMap, _ := poolMap.Get(queue)
		if queueMap == nil {
			queueMap = immutable.NewMap[string, internaltypes.ResourceList](nil)
		}
		current, _ := queueMap.Get(pc)
		a.queuedDemand = a.queuedDemand.Set(pool, poolMap.Set(queue, queueMap.Set(pc, current.Add(req))))
		countPoolMap, _ := a.queuedCounts.Get(pool)
		if countPoolMap == nil {
			countPoolMap = immutable.NewMap[string, *immutable.Map[string, int]](nil)
		}
		countQueueMap, _ := countPoolMap.Get(queue)
		if countQueueMap == nil {
			countQueueMap = immutable.NewMap[string, int](nil)
		}
		count, _ := countQueueMap.Get(pc)
		a.queuedCounts = a.queuedCounts.Set(pool, countPoolMap.Set(queue, countQueueMap.Set(pc, count+1)))
	}
}

// remove removes job from the aggregate. The job must be in the same state as
// when it was added; any inconsistency is reported as an invariant violation.
func (a *JobAggregate) remove(job *Job) {
	if a == nil || job == nil || job.InTerminalState() || !job.Queued() {
		return
	}

	req := job.AllResourceRequirements()
	queue := job.Queue()
	pc := job.PriorityClassName()
	pools := job.pools
	for i, pool := range pools {
		if slices.Contains(pools[:i], pool) {
			continue
		}
		poolMap, ok := a.queuedDemand.Get(pool)
		if !ok || poolMap == nil {
			recordAggregateInvariantViolation("remove_missing_pool")
			continue
		}
		queueMap, ok := poolMap.Get(queue)
		if !ok || queueMap == nil {
			recordAggregateInvariantViolation("remove_missing_queue")
			continue
		}
		current, ok := queueMap.Get(pc)
		if !ok {
			recordAggregateInvariantViolation("remove_missing_priority_class")
			continue
		}
		countPoolMap, ok := a.queuedCounts.Get(pool)
		if !ok || countPoolMap == nil {
			recordAggregateInvariantViolation("remove_missing_count_pool")
			continue
		}
		countQueueMap, ok := countPoolMap.Get(queue)
		if !ok || countQueueMap == nil {
			recordAggregateInvariantViolation("remove_missing_count_queue")
			continue
		}
		count, ok := countQueueMap.Get(pc)
		if !ok || count <= 0 {
			recordAggregateInvariantViolation("remove_missing_count")
			continue
		}
		remaining := current.Subtract(req)
		if remaining.HasNegativeValues() {
			recordAggregateInvariantViolation("remove_negative_remaining")
			continue
		}
		if count == 1 {
			queueMap = queueMap.Delete(pc)
			countQueueMap = countQueueMap.Delete(pc)
		} else {
			queueMap = queueMap.Set(pc, remaining)
			countQueueMap = countQueueMap.Set(pc, count-1)
		}
		if queueMap.Len() == 0 {
			poolMap = poolMap.Delete(queue)
		} else {
			poolMap = poolMap.Set(queue, queueMap)
		}
		if countQueueMap.Len() == 0 {
			countPoolMap = countPoolMap.Delete(queue)
		} else {
			countPoolMap = countPoolMap.Set(queue, countQueueMap)
		}
		if poolMap.Len() == 0 {
			a.queuedDemand = a.queuedDemand.Delete(pool)
		} else {
			a.queuedDemand = a.queuedDemand.Set(pool, poolMap)
		}
		if countPoolMap.Len() == 0 {
			a.queuedCounts = a.queuedCounts.Delete(pool)
		} else {
			a.queuedCounts = a.queuedCounts.Set(pool, countPoolMap)
		}
	}
}

// getQueueDemand returns queued demand for the given pool and queue by priority
// class. It does not know about cordoned queues; callers decide whether to
// query a queue at all.
func (a *JobAggregate) getQueueDemand(pool string, queue string) map[string]internaltypes.ResourceList {
	if a == nil || a.queuedDemand == nil {
		return map[string]internaltypes.ResourceList{}
	}
	poolMap, ok := a.queuedDemand.Get(pool)
	if !ok || poolMap == nil {
		return map[string]internaltypes.ResourceList{}
	}
	queueMap, ok := poolMap.Get(queue)
	if !ok || queueMap == nil {
		return map[string]internaltypes.ResourceList{}
	}
	demand := make(map[string]internaltypes.ResourceList, queueMap.Len())
	queueIt := queueMap.Iterator()
	for !queueIt.Done() {
		pc, rl, _ := queueIt.Next()
		demand[pc] = rl
	}
	return demand
}
