package jobdb

import (
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

// SchedulingInfo is the per-pool scheduling information derived from a JobAggregate.
// It mirrors the information the scheduler currently computes by scanning every job in
// the JobDb (see calculateJobSchedulingInfo in the scheduling package).
type SchedulingInfo struct {
	// Jobs leased to each executor, restricted to the pools relevant to the round.
	JobsByExecutorId map[string][]*Job
	// Jobs leased to each pool.
	JobsByPool map[string][]*Job
	// Demand, i.e., the sum of the resource requirements of all jobs eligible for the pool.
	DemandByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList
	// Allocation, i.e., the sum of the resource requirements of jobs leased to the pool.
	AllocatedByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList
	// Away allocation, i.e., the sum of the resource requirements of jobs leased to away pools.
	AwayAllocatedByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList
	// Priority classes used by any active job.
	InUsePriorityClasses map[string]bool
}

// JobAggregate maintains incrementally-updated, per-pool aggregates of the active jobs in
// the JobDb. Its purpose is to let the scheduler derive the information it needs to make
// scheduling decisions without re-scanning every job on every scheduling round. This is
// particularly beneficial for demand/allocation accounting, which otherwise requires
// iterating over all queued jobs for every pool.
//
// The aggregate is maintained on JobDb mutations (Upsert/delete). It uses copy-on-write at
// the pool granularity so that write transactions can mutate it without affecting the
// committed state until Commit is called, mirroring the rest of the JobDb.
type JobAggregate struct {
	// Per-pool aggregates. A pool is only present while it has at least one job.
	byPool map[string]*poolAggregate
	// Number of active jobs per queue and priority class. Counts (rather than a set) are used
	// so that removing one job does not incorrectly drop a priority class still used by other
	// jobs. Keying by queue lets the query drop priority classes of queues that no longer exist.
	inUsePriorityClassCounts map[string]map[string]int
	// Pools that this aggregate instance has cloned and may therefore mutate in place.
	// Pools not in this set are shared with another aggregate and must be cloned before use.
	ownedPools map[string]bool
}

// poolAggregate holds the aggregates for a single pool.
type poolAggregate struct {
	// Resources requested by queued jobs eligible for this pool. Cordon sensitive.
	queuedByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList
	// Resources requested by jobs that are neither queued nor leased (i.e., running jobs
	// without a run). These only contribute to demand and are never cordon sensitive.
	unleasedDemandByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList
	// Resources requested by all jobs leased to this pool, across all executors.
	// Used for demand, which includes running jobs.
	leasedByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList
	// Resources requested by jobs leased to this pool, grouped by executor.
	// Used for allocation, which only counts jobs on active executors.
	allocatedByExecutor map[string]map[string]map[string]internaltypes.ResourceList
	// Jobs leased to this pool, keyed by job id.
	leasedJobs map[string]*Job
}

func NewJobAggregate() *JobAggregate {
	return &JobAggregate{
		byPool:                   map[string]*poolAggregate{},
		inUsePriorityClassCounts: map[string]map[string]int{},
		ownedPools:               map[string]bool{},
	}
}

func newPoolAggregate() *poolAggregate {
	return &poolAggregate{
		queuedByQueueAndPriorityClass:         map[string]map[string]internaltypes.ResourceList{},
		unleasedDemandByQueueAndPriorityClass: map[string]map[string]internaltypes.ResourceList{},
		leasedByQueueAndPriorityClass:         map[string]map[string]internaltypes.ResourceList{},
		allocatedByExecutor:                   map[string]map[string]map[string]internaltypes.ResourceList{},
		leasedJobs:                            map[string]*Job{},
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
		byPool:                   maps.Clone(a.byPool),
		inUsePriorityClassCounts: clonePriorityClassCounts(a.inUsePriorityClassCounts),
		ownedPools:               map[string]bool{},
	}
}

func clonePriorityClassCounts(m map[string]map[string]int) map[string]map[string]int {
	clone := make(map[string]map[string]int, len(m))
	for queue, counts := range m {
		clone[queue] = maps.Clone(counts)
	}
	return clone
}

func (p *poolAggregate) clone() *poolAggregate {
	if p == nil {
		return newPoolAggregate()
	}
	return &poolAggregate{
		queuedByQueueAndPriorityClass:         cloneQueuePriorityResourceMap(p.queuedByQueueAndPriorityClass),
		unleasedDemandByQueueAndPriorityClass: cloneQueuePriorityResourceMap(p.unleasedDemandByQueueAndPriorityClass),
		leasedByQueueAndPriorityClass:         cloneQueuePriorityResourceMap(p.leasedByQueueAndPriorityClass),
		allocatedByExecutor:                   cloneExecutorQueuePriorityResourceMap(p.allocatedByExecutor),
		leasedJobs:                            maps.Clone(p.leasedJobs),
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

// Add incorporates job into the aggregate. Jobs that are nil or in a terminal state are
// ignored, matching the scheduler's own filtering.
func (a *JobAggregate) Add(job *Job) {
	if job == nil || job.InTerminalState() {
		return
	}

	pc := job.PriorityClassName()
	addPriorityClassCount(a.inUsePriorityClassCounts, job.Queue(), pc)

	req := job.AllResourceRequirements()

	if job.Queued() {
		for _, pool := range job.Pools() {
			pa := a.ensureOwned(pool)
			addQueuePriorityResource(pa.queuedByQueueAndPriorityClass, job.Queue(), pc, req)
		}
		return
	}

	run := job.LatestRun()
	if run == nil {
		// Not queued and no run: only contributes to demand (for the pools it is eligible for).
		for _, pool := range job.Pools() {
			pa := a.ensureOwned(pool)
			addQueuePriorityResource(pa.unleasedDemandByQueueAndPriorityClass, job.Queue(), pc, req)
		}
		return
	}

	pool := run.Pool()
	executor := run.Executor()
	pa := a.ensureOwned(pool)
	addQueuePriorityResource(pa.leasedByQueueAndPriorityClass, job.Queue(), pc, req)
	addExecutorQueuePriorityResource(pa.allocatedByExecutor, executor, job.Queue(), pc, req)
	pa.leasedJobs[job.Id()] = job
}

// Remove removes job from the aggregate. It is the inverse of Add and must be called with
// the job's state as it was when it was added.
func (a *JobAggregate) Remove(job *Job) {
	if job == nil || job.InTerminalState() {
		return
	}

	pc := job.PriorityClassName()
	subPriorityClassCount(a.inUsePriorityClassCounts, job.Queue(), pc)

	req := job.AllResourceRequirements()

	if job.Queued() {
		for _, pool := range job.Pools() {
			if pa := a.ownedPoolIfPresent(pool); pa != nil {
				subQueuePriorityResource(pa.queuedByQueueAndPriorityClass, job.Queue(), pc, req)
				a.dropPoolIfEmpty(pool, pa)
			}
		}
		return
	}

	run := job.LatestRun()
	if run == nil {
		for _, pool := range job.Pools() {
			if pa := a.ownedPoolIfPresent(pool); pa != nil {
				subQueuePriorityResource(pa.unleasedDemandByQueueAndPriorityClass, job.Queue(), pc, req)
				a.dropPoolIfEmpty(pool, pa)
			}
		}
		return
	}

	pool := run.Pool()
	if pa := a.ownedPoolIfPresent(pool); pa != nil {
		subQueuePriorityResource(pa.leasedByQueueAndPriorityClass, job.Queue(), pc, req)
		subExecutorQueuePriorityResource(pa.allocatedByExecutor, run.Executor(), job.Queue(), pc, req)
		delete(pa.leasedJobs, job.Id())
		a.dropPoolIfEmpty(pool, pa)
	}
}

func (a *JobAggregate) dropPoolIfEmpty(pool string, pa *poolAggregate) {
	if pa.isEmpty() {
		delete(a.byPool, pool)
		delete(a.ownedPools, pool)
	}
}

func (p *poolAggregate) isEmpty() bool {
	return len(p.queuedByQueueAndPriorityClass) == 0 &&
		len(p.unleasedDemandByQueueAndPriorityClass) == 0 &&
		len(p.leasedByQueueAndPriorityClass) == 0 &&
		len(p.allocatedByExecutor) == 0 &&
		len(p.leasedJobs) == 0
}

// CalculateSchedulingInfo derives the per-pool scheduling information for the pool described
// by currentPool, awayAllocationPools and allPools from the aggregate. It is intended to be
// a drop-in (and much cheaper) replacement for scanning every job.
//
// knownQueues is used to discard jobs whose queue no longer exists, and cordonedQueues to
// exclude queued jobs on cordoned queues from demand, matching the legacy calculation.
func (a *JobAggregate) CalculateSchedulingInfo(
	activeExecutorsSet map[string]bool,
	currentPool string,
	awayAllocationPools []string,
	allPools []string,
	knownQueues map[string]bool,
	cordonedQueues map[string]bool,
) *SchedulingInfo {
	info := &SchedulingInfo{
		JobsByExecutorId:                     map[string][]*Job{},
		JobsByPool:                           map[string][]*Job{},
		DemandByQueueAndPriorityClass:        map[string]map[string]internaltypes.ResourceList{},
		AllocatedByQueueAndPriorityClass:     map[string]map[string]internaltypes.ResourceList{},
		AwayAllocatedByQueueAndPriorityClass: map[string]map[string]internaltypes.ResourceList{},
		InUsePriorityClasses:                 map[string]bool{},
	}
	for queue, byPriorityClass := range a.inUsePriorityClassCounts {
		if !queueKnown(knownQueues, queue) {
			continue
		}
		for pc, count := range byPriorityClass {
			if count > 0 {
				info.InUsePriorityClasses[pc] = true
			}
		}
	}

	allPoolsSet := make(map[string]bool, len(allPools))
	for _, pool := range allPools {
		allPoolsSet[pool] = true
	}
	awayPoolsSet := make(map[string]bool, len(awayAllocationPools))
	for _, pool := range awayAllocationPools {
		awayPoolsSet[pool] = true
	}

	for pool, pa := range a.byPool {
		if len(pa.leasedJobs) > 0 {
			jobs := make([]*Job, 0, len(pa.leasedJobs))
			for _, job := range pa.leasedJobs {
				if !queueKnown(knownQueues, job.Queue()) {
					continue
				}
				jobs = append(jobs, job)
				if allPoolsSet[pool] {
					executor := job.LatestRun().Executor()
					info.JobsByExecutorId[executor] = append(info.JobsByExecutorId[executor], job)
				}
			}
			if len(jobs) > 0 {
				info.JobsByPool[pool] = jobs
			}
		}

		if pool == currentPool {
			mergeResourceMaps(info.DemandByQueueAndPriorityClass, pa.leasedByQueueAndPriorityClass, knownQueues)
			mergeResourceMaps(info.DemandByQueueAndPriorityClass, pa.unleasedDemandByQueueAndPriorityClass, knownQueues)
			for queue, byPriorityClass := range pa.queuedByQueueAndPriorityClass {
				if cordonedQueues[queue] || !queueKnown(knownQueues, queue) {
					continue
				}
				mergeResourceMapForQueue(info.DemandByQueueAndPriorityClass, queue, byPriorityClass)
			}
			mergeActiveExecutorResourceMaps(info.AllocatedByQueueAndPriorityClass, pa.allocatedByExecutor, activeExecutorsSet, knownQueues)
		} else if awayPoolsSet[pool] {
			mergeActiveExecutorResourceMaps(info.AwayAllocatedByQueueAndPriorityClass, pa.allocatedByExecutor, activeExecutorsSet, knownQueues)
		}
	}

	return info
}

func addPriorityClassCount(m map[string]map[string]int, queue, priorityClass string) {
	byPriorityClass, ok := m[queue]
	if !ok {
		byPriorityClass = map[string]int{}
		m[queue] = byPriorityClass
	}
	byPriorityClass[priorityClass]++
}

func subPriorityClassCount(m map[string]map[string]int, queue, priorityClass string) {
	byPriorityClass, ok := m[queue]
	if !ok {
		return
	}
	if count := byPriorityClass[priorityClass]; count > 1 {
		byPriorityClass[priorityClass] = count - 1
	} else {
		delete(byPriorityClass, priorityClass)
	}
	if len(byPriorityClass) == 0 {
		delete(m, queue)
	}
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

func addExecutorQueuePriorityResource(
	m map[string]map[string]map[string]internaltypes.ResourceList,
	executor string,
	queue string,
	priorityClass string,
	rl internaltypes.ResourceList,
) {
	byQueue, ok := m[executor]
	if !ok {
		byQueue = map[string]map[string]internaltypes.ResourceList{}
		m[executor] = byQueue
	}
	addQueuePriorityResource(byQueue, queue, priorityClass, rl)
}

func subExecutorQueuePriorityResource(
	m map[string]map[string]map[string]internaltypes.ResourceList,
	executor string,
	queue string,
	priorityClass string,
	rl internaltypes.ResourceList,
) {
	byQueue, ok := m[executor]
	if !ok {
		return
	}
	subQueuePriorityResource(byQueue, queue, priorityClass, rl)
	if len(byQueue) == 0 {
		delete(m, executor)
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

func cloneExecutorQueuePriorityResourceMap(
	m map[string]map[string]map[string]internaltypes.ResourceList,
) map[string]map[string]map[string]internaltypes.ResourceList {
	if m == nil {
		return map[string]map[string]map[string]internaltypes.ResourceList{}
	}
	clone := make(map[string]map[string]map[string]internaltypes.ResourceList, len(m))
	for executor, byQueue := range m {
		clone[executor] = cloneQueuePriorityResourceMap(byQueue)
	}
	return clone
}

func mergeResourceMaps(
	dst map[string]map[string]internaltypes.ResourceList,
	src map[string]map[string]internaltypes.ResourceList,
	knownQueues map[string]bool,
) {
	for queue, byPriorityClass := range src {
		if !queueKnown(knownQueues, queue) {
			continue
		}
		mergeResourceMapForQueue(dst, queue, byPriorityClass)
	}
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

func mergeActiveExecutorResourceMaps(
	dst map[string]map[string]internaltypes.ResourceList,
	src map[string]map[string]map[string]internaltypes.ResourceList,
	activeExecutorsSet map[string]bool,
	knownQueues map[string]bool,
) {
	for executor, byQueue := range src {
		if !activeExecutorsSet[executor] {
			continue
		}
		mergeResourceMaps(dst, byQueue, knownQueues)
	}
}
