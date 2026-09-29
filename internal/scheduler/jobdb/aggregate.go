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
}

func NewJobAggregate() *JobAggregate {
	return &JobAggregate{
		queuedDemand: immutable.NewMap[string, *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]]](nil),
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
		remaining := current.Subtract(req)
		if remaining.HasNegativeValues() {
			recordAggregateInvariantViolation("remove_negative_remaining")
			continue
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
			a.queuedDemand = a.queuedDemand.Delete(pool)
		} else {
			a.queuedDemand = a.queuedDemand.Set(pool, poolMap)
		}
	}
}

// getQueuedDemand returns queued demand for currentPool by queue and priority
// class. Unknown and cordoned queues are excluded.
func (a *JobAggregate) getQueuedDemand(
	currentPool string,
	knownQueues map[string]bool,
	cordonedQueues map[string]bool,
) map[string]map[string]internaltypes.ResourceList {
	if a == nil || a.queuedDemand == nil {
		return map[string]map[string]internaltypes.ResourceList{}
	}
	poolMap, ok := a.queuedDemand.Get(currentPool)
	if !ok || poolMap == nil {
		return map[string]map[string]internaltypes.ResourceList{}
	}
	demand := make(map[string]map[string]internaltypes.ResourceList, poolMap.Len())
	poolIt := poolMap.Iterator()
	for !poolIt.Done() {
		queue, queueMap, _ := poolIt.Next()
		if queueMap == nil || cordonedQueues[queue] || !queueKnown(knownQueues, queue) {
			continue
		}
		byPriorityClass, ok := demand[queue]
		if !ok {
			byPriorityClass = make(map[string]internaltypes.ResourceList, queueMap.Len())
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

func queueKnown(knownQueues map[string]bool, queue string) bool {
	if knownQueues == nil {
		return true
	}
	return knownQueues[queue]
}
