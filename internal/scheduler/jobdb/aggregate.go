package jobdb

import (
	"github.com/benbjohnson/immutable"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

// aggregateKey identifies a queued-demand aggregate entry.
type aggregateKey struct {
	pool          string
	queue         string
	priorityClass string
}

type aggregateKeyHasher struct{}

func (aggregateKeyHasher) Hash(key aggregateKey) uint32 {
	var hash uint32
	for _, part := range []string{key.pool, key.queue, key.priorityClass} {
		hash = 31*hash + 7
		for _, c := range part {
			hash = 31*hash + uint32(c)
		}
	}
	return hash
}

func (aggregateKeyHasher) Equal(a, b aggregateKey) bool {
	return a == b
}

// JobAggregate maintains an incrementally updated aggregate of queued demand by
// pool, queue and priority class, so the scheduler can read queued demand
// without scanning every queued job.
type JobAggregate struct {
	byKey *immutable.Map[aggregateKey, internaltypes.ResourceList]
}

func NewJobAggregate() *JobAggregate {
	return &JobAggregate{
		byKey: immutable.NewMap[aggregateKey, internaltypes.ResourceList](aggregateKeyHasher{}),
	}
}

// Clone returns a copy of the aggregate for use by a write transaction. The copy
// shares the current state; changes to it are not visible until committed.
func (a *JobAggregate) Clone() *JobAggregate {
	if a == nil {
		return NewJobAggregate()
	}
	return &JobAggregate{
		byKey: a.byKey,
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
	forEachDistinctPool(job.pools, func(pool string) {
		key := aggregateKey{pool: pool, queue: queue, priorityClass: pc}
		current, _ := a.byKey.Get(key)
		a.byKey = a.byKey.Set(key, current.Add(req))
	})
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
	forEachDistinctPool(job.pools, func(pool string) {
		key := aggregateKey{pool: pool, queue: queue, priorityClass: pc}
		current, ok := a.byKey.Get(key)
		if !ok {
			recordAggregateInvariantViolation("remove_missing_entry")
			return
		}
		remaining := current.Subtract(req)
		if remaining.HasNegativeValues() {
			recordAggregateInvariantViolation("remove_negative_remaining")
			return
		}
		if remaining.AllZero() {
			a.byKey = a.byKey.Delete(key)
		} else {
			a.byKey = a.byKey.Set(key, remaining)
		}
	})
}

// getQueuedDemand returns queued demand for currentPool by queue and priority
// class. Unknown and cordoned queues are excluded.
func (a *JobAggregate) getQueuedDemand(
	currentPool string,
	knownQueues map[string]bool,
	cordonedQueues map[string]bool,
) map[string]map[string]internaltypes.ResourceList {
	demand := map[string]map[string]internaltypes.ResourceList{}
	if a == nil || a.byKey == nil {
		return demand
	}
	it := a.byKey.Iterator()
	for !it.Done() {
		key, rl, _ := it.Next()
		if key.pool != currentPool || cordonedQueues[key.queue] || !queueKnown(knownQueues, key.queue) {
			continue
		}
		byPriorityClass, ok := demand[key.queue]
		if !ok {
			byPriorityClass = map[string]internaltypes.ResourceList{}
			demand[key.queue] = byPriorityClass
		}
		byPriorityClass[key.priorityClass] = byPriorityClass[key.priorityClass].Add(rl)
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
