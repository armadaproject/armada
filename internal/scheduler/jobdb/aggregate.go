package jobdb

import (
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

// JobAggregate maintains an incrementally-updated aggregate of queued jobs in
// the JobDb. Its sole purpose is to let the scheduler derive queued demand
// without scanning every queued job on every scheduling round.
//
// Mutation discipline mirrors NodeDb:
//   - The caller owns the transaction lifecycle (JobDb.WriteTxn/ReadTxn/
//     DryRunTxn, followed by Commit/Abort). The aggregate never creates its
//     own transaction.
//   - Mutating operations (add/remove) are private and only invoked from the
//     write-transaction path (Txn.Upsert/delete), which requires a writable
//     transaction. They use copy-on-write at the pool granularity so a write
//     transaction never affects committed state until Commit.
//   - Read operations (GetQueuedDemand) are pure and never mutate the
//     aggregate, mirroring NodeDb's SelectNodeForJobWithTxn which takes a txn
//     but does not mutate the db.
type JobAggregate struct {
	// Per-pool aggregates. A pool is only present while it has at least one queued job.
	byPool map[string]*poolAggregate
	// Pools that this aggregate instance has cloned and may therefore mutate in place.
	// Pools not in this set are shared with another aggregate and must be cloned before use.
	ownedPools map[string]bool
}

// poolAggregate holds the queued-job aggregate for a single pool.
type poolAggregate struct {
	// Resources requested by queued jobs eligible for this pool. Cordon sensitive at read time.
	queuedByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList
}

func NewJobAggregate() *JobAggregate {
	return &JobAggregate{
		byPool:     map[string]*poolAggregate{},
		ownedPools: map[string]bool{},
	}
}

func newPoolAggregate() *poolAggregate {
	return &poolAggregate{
		queuedByQueueAndPriorityClass: map[string]map[string]internaltypes.ResourceList{},
	}
}

// Clone returns a copy of the aggregate suitable for mutation by a write transaction.
// The copy is shallow: pool aggregates are cloned lazily on first mutation, so cloning is
// cheap even when the aggregate holds a large number of jobs.
func (a *JobAggregate) Clone() *JobAggregate {
	if a == nil {
		return NewJobAggregate()
	}
	return &JobAggregate{
		byPool:     maps.Clone(a.byPool),
		ownedPools: map[string]bool{},
	}
}

func (p *poolAggregate) clone() *poolAggregate {
	if p == nil {
		return newPoolAggregate()
	}
	return &poolAggregate{
		queuedByQueueAndPriorityClass: cloneQueuePriorityResourceMap(p.queuedByQueueAndPriorityClass),
	}
}

// ensureOwned returns the pool aggregate for pool, creating it if necessary and cloning it
// first if it is still shared with the aggregate this instance was cloned from.
func (a *JobAggregate) ensureOwned(pool string) *poolAggregate {
	if pa, ok := a.byPool[pool]; ok {
		if a.ownedPools[pool] {
			return pa
		}
		cloned := pa.clone()
		a.byPool[pool] = cloned
		a.ownedPools[pool] = true
		return cloned
	}
	pa := newPoolAggregate()
	a.byPool[pool] = pa
	a.ownedPools[pool] = true
	return pa
}

// ownedPoolIfPresent returns the pool aggregate for pool if it exists, cloning it first if
// it is still shared. Returns nil if the pool has no aggregate.
func (a *JobAggregate) ownedPoolIfPresent(pool string) *poolAggregate {
	pa, ok := a.byPool[pool]
	if !ok {
		return nil
	}
	if a.ownedPools[pool] {
		return pa
	}
	cloned := pa.clone()
	a.byPool[pool] = cloned
	a.ownedPools[pool] = true
	return cloned
}

// add incorporates job into the aggregate. Only queued, non-terminal jobs contribute;
// all other jobs are ignored. It must only be called from the write-transaction path.
func (a *JobAggregate) add(job *Job) {
	if job == nil || job.InTerminalState() || !job.Queued() {
		return
	}

	req := job.AllResourceRequirements()
	for _, pool := range job.Pools() {
		pa := a.ensureOwned(pool)
		addQueuePriorityResource(pa.queuedByQueueAndPriorityClass, job.Queue(), job.PriorityClassName(), req)
	}
}

// remove removes job from the aggregate. It is the inverse of add and must be called with
// the job's state as it was when it was added. It must only be called from the
// write-transaction path.
func (a *JobAggregate) remove(job *Job) {
	if job == nil || job.InTerminalState() || !job.Queued() {
		return
	}

	req := job.AllResourceRequirements()
	for _, pool := range job.Pools() {
		if pa := a.ownedPoolIfPresent(pool); pa != nil {
			subQueuePriorityResource(pa.queuedByQueueAndPriorityClass, job.Queue(), job.PriorityClassName(), req)
			a.dropPoolIfEmpty(pool, pa)
		}
	}
}

func (a *JobAggregate) dropPoolIfEmpty(pool string, pa *poolAggregate) {
	if pa.isEmpty() {
		delete(a.byPool, pool)
		delete(a.ownedPools, pool)
	}
}

func (p *poolAggregate) isEmpty() bool {
	return len(p.queuedByQueueAndPriorityClass) == 0
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
	pa, ok := a.byPool[currentPool]
	if !ok {
		return demand
	}
	for queue, byPriorityClass := range pa.queuedByQueueAndPriorityClass {
		if cordonedQueues[queue] || !queueKnown(knownQueues, queue) {
			continue
		}
		mergeResourceMapForQueue(demand, queue, byPriorityClass)
	}
	return demand
}

func queueKnown(knownQueues map[string]bool, queue string) bool {
	if knownQueues == nil {
		return true
	}
	return knownQueues[queue]
}

func addQueuePriorityResource(
	m map[string]map[string]internaltypes.ResourceList,
	queue string,
	priorityClass string,
	rl internaltypes.ResourceList,
) {
	byPriorityClass, ok := m[queue]
	if !ok {
		byPriorityClass = map[string]internaltypes.ResourceList{}
		m[queue] = byPriorityClass
	}
	byPriorityClass[priorityClass] = byPriorityClass[priorityClass].Add(rl)
}

func subQueuePriorityResource(
	m map[string]map[string]internaltypes.ResourceList,
	queue string,
	priorityClass string,
	rl internaltypes.ResourceList,
) {
	byPriorityClass, ok := m[queue]
	if !ok {
		return
	}
	remaining := byPriorityClass[priorityClass].Subtract(rl)
	if remaining.AllZero() {
		delete(byPriorityClass, priorityClass)
	} else {
		byPriorityClass[priorityClass] = remaining
	}
	if len(byPriorityClass) == 0 {
		delete(m, queue)
	}
}

func cloneQueuePriorityResourceMap(
	m map[string]map[string]internaltypes.ResourceList,
) map[string]map[string]internaltypes.ResourceList {
	if m == nil {
		return map[string]map[string]internaltypes.ResourceList{}
	}
	clone := make(map[string]map[string]internaltypes.ResourceList, len(m))
	for queue, byPriorityClass := range m {
		clone[queue] = maps.Clone(byPriorityClass)
	}
	return clone
}

func mergeResourceMapForQueue(
	dst map[string]map[string]internaltypes.ResourceList,
	queue string,
	byPriorityClass map[string]internaltypes.ResourceList,
) {
	dstByPriorityClass, ok := dst[queue]
	if !ok {
		dstByPriorityClass = map[string]internaltypes.ResourceList{}
		dst[queue] = dstByPriorityClass
	}
	for priorityClass, rl := range byPriorityClass {
		dstByPriorityClass[priorityClass] = dstByPriorityClass[priorityClass].Add(rl)
	}
}
