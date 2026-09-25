package jobdb

import (
	"github.com/benbjohnson/immutable"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

// JobAggregate maintains an incrementally-updated aggregate of queued jobs in
// the JobDb. Its sole purpose is to let the scheduler derive queued demand
// without scanning every queued job on every scheduling round.
//
// It follows the same pattern as the rest of the JobDb state (jobsById,
// leasedJobs, ...), which is built on github.com/benbjohnson/immutable:
//   - The underlying maps are persistent: Set/Delete return a new map and
//     never mutate the map they are called on.
//   - Read transactions share the aggregate pointer directly; no cloning is
//     needed because reads never mutate.
//   - Write transactions hold their own JobAggregate wrapper (via the cheap
//     O(1) Clone, which only shares the inner map pointer) and mutate the
//     wrapper in place. The committed state is unaffected until Commit
//     publishes txn.aggregate, exactly like txn.jobsById.
//   - The caller owns the transaction lifecycle (JobDb.WriteTxn/ReadTxn/
//     DryRunTxn, followed by Commit/Abort). The aggregate never creates its
//     own transaction, mirroring NodeDb's WithTxn mutation discipline where
//     mutating methods take the caller's txn instead of creating one.
type JobAggregate struct {
	// Queued demand by pool, then queue, then priority class.
	// Persistent: never mutated in place, only replaced via Set/Delete.
	byPool *immutable.Map[string, *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]]]
}

func NewJobAggregate() *JobAggregate {
	return &JobAggregate{
		byPool: immutable.NewMap[string, *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]]](nil),
	}
}

// Clone returns a wrapper suitable for mutation by a write transaction.
// It is O(1): only the wrapper is new, the underlying persistent map is shared.
// This mirrors how WriteTxn shares jobsById and other immutable state.
func (a *JobAggregate) Clone() *JobAggregate {
	if a == nil {
		return NewJobAggregate()
	}
	return &JobAggregate{
		byPool: a.byPool,
	}
}

// add incorporates job into the aggregate. Only queued, non-terminal jobs contribute;
// all other jobs are ignored. It must only be called from the write-transaction path,
// on a JobAggregate wrapper owned by that transaction.
func (a *JobAggregate) add(job *Job) {
	if a == nil || job == nil || job.InTerminalState() || !job.Queued() {
		return
	}

	req := job.AllResourceRequirements()
	queue := job.Queue()
	pc := job.PriorityClassName()
	for _, pool := range job.Pools() {
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
	}
}

// remove removes job from the aggregate. It is the inverse of add and must be called with
// the job's state as it was when it was added. It must only be called from the
// write-transaction path, on a JobAggregate wrapper owned by that transaction.
func (a *JobAggregate) remove(job *Job) {
	if a == nil || job == nil || job.InTerminalState() || !job.Queued() {
		return
	}

	req := job.AllResourceRequirements()
	queue := job.Queue()
	pc := job.PriorityClassName()
	for _, pool := range job.Pools() {
		poolMap, ok := a.byPool.Get(pool)
		if !ok || poolMap == nil {
			continue
		}
		queueMap, ok := poolMap.Get(queue)
		if !ok || queueMap == nil {
			continue
		}
		current, ok := queueMap.Get(pc)
		if !ok {
			continue
		}
		remaining := current.Subtract(req)
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
	}
}

// getQueuedDemand is a pure read: it never mutates the aggregate. knownQueues discards
// jobs whose queue no longer exists, and cordonedQueues excludes queued jobs on cordoned
// queues, matching the legacy calculation.
func (a *JobAggregate) getQueuedDemand(
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

func queueKnown(knownQueues map[string]bool, queue string) bool {
	if knownQueues == nil {
		return true
	}
	return knownQueues[queue]
}
