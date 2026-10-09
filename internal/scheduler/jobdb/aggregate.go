package jobdb

import (
	"github.com/benbjohnson/immutable"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

// SchedulingInfo is the per-pool scheduling information derived from a
// JobAggregate. It mirrors the information the scheduler computes by scanning
// every job (see calculateJobSchedulingInfo in the scheduling package).
type SchedulingInfo struct {
	// Jobs leased to each executor, restricted to the pools relevant to the round.
	JobsByExecutorId map[string][]*Job
	// Jobs leased to each pool.
	JobsByPool map[string][]*Job
	// Demand, i.e., the sum of the resource requirements of all jobs eligible for
	// the pool. Includes queued and running jobs.
	DemandByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList
	// Allocation, i.e., the sum of the resource requirements of jobs leased to the
	// pool on active executors.
	AllocatedByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList
	// Away allocation, i.e., the sum of the resource requirements of jobs leased to
	// away pools on active executors.
	AwayAllocatedByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList
	// Priority classes used by any active job.
	InUsePriorityClasses map[string]bool
}

// JobAggregate maintains an incrementally updated aggregate of the active jobs
// in the JobDb so the scheduler can derive the information it needs to make
// scheduling decisions without scanning every job. This covers queued demand as
// well as running (leased) demand, allocation and the leased jobs themselves.
type JobAggregate struct {
	// Queued demand by pool, then queue, then priority class.
	queuedDemand *immutable.Map[string, *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]]]
	// Allocation by pool, then executor, then queue, then priority class. Running
	// demand and in-use priority classes are derived from this.
	allocatedByExecutor *immutable.Map[string, *immutable.Map[string, *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]]]]
	// Leased jobs by job id.
	leasedJobs *immutable.Map[string, *Job]
}

func NewJobAggregate() *JobAggregate {
	return &JobAggregate{
		queuedDemand:        immutable.NewMap[string, *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]]](nil),
		allocatedByExecutor: immutable.NewMap[string, *immutable.Map[string, *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]]]](nil),
		leasedJobs:          immutable.NewMap[string, *Job](nil),
	}
}

// Clone returns a copy of the aggregate for use by a write transaction. The copy
// shares the current state; changes to it are not visible until committed.
func (a *JobAggregate) Clone() *JobAggregate {
	if a == nil {
		return NewJobAggregate()
	}
	return &JobAggregate{
		queuedDemand:        a.queuedDemand,
		allocatedByExecutor: a.allocatedByExecutor,
		leasedJobs:          a.leasedJobs,
	}
}

// add incorporates job into the aggregate. Jobs that are in a terminal state are
// ignored. Queued jobs contribute queued demand for every pool they are eligible
// for; leased (running) jobs contribute running demand and allocation for the
// pool and executor of their current run; jobs that are neither queued nor
// leased are ignored, matching the scheduler's own filtering.
func (a *JobAggregate) add(job *Job) {
	if a == nil || job == nil || job.InTerminalState() {
		return
	}
	if job.Queued() {
		a.addQueued(job)
		return
	}
	if run := job.LatestRun(); run != nil {
		a.addLeased(job, run)
	}
}

// remove removes job from the aggregate. The job must be in the same state as
// when it was added; any inconsistency is reported as an invariant violation.
func (a *JobAggregate) remove(job *Job) {
	if a == nil || job == nil || job.InTerminalState() {
		return
	}
	if job.Queued() {
		a.removeQueued(job)
		return
	}
	if run := job.LatestRun(); run != nil {
		a.removeLeased(job, run)
	}
}

func (a *JobAggregate) addQueued(job *Job) {
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
		a.queuedDemand = a.queuedDemand.Set(pool, addQueuePriority(poolMap, queue, pc, req))
	}
}

func (a *JobAggregate) removeQueued(job *Job) {
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
		poolMap, ok = subQueuePriority(poolMap, queue, pc, req)
		if !ok {
			continue
		}
		if poolMap.Len() == 0 {
			a.queuedDemand = a.queuedDemand.Delete(pool)
		} else {
			a.queuedDemand = a.queuedDemand.Set(pool, poolMap)
		}
	}
}

func (a *JobAggregate) addLeased(job *Job, run *JobRun) {
	req := job.AllResourceRequirements()
	queue := job.Queue()
	pc := job.PriorityClassName()
	pool := run.Pool()
	executor := run.Executor()

	poolMap, _ := a.allocatedByExecutor.Get(pool)
	if poolMap == nil {
		poolMap = immutable.NewMap[string, *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]]](nil)
	}
	executorMap, _ := poolMap.Get(executor)
	if executorMap == nil {
		executorMap = immutable.NewMap[string, *immutable.Map[string, internaltypes.ResourceList]](nil)
	}
	a.allocatedByExecutor = a.allocatedByExecutor.Set(pool, poolMap.Set(executor, addQueuePriority(executorMap, queue, pc, req)))
	a.leasedJobs = a.leasedJobs.Set(job.Id(), job)
}

func (a *JobAggregate) removeLeased(job *Job, run *JobRun) {
	req := job.AllResourceRequirements()
	queue := job.Queue()
	pc := job.PriorityClassName()
	pool := run.Pool()
	executor := run.Executor()

	poolMap, ok := a.allocatedByExecutor.Get(pool)
	if !ok || poolMap == nil {
		recordAggregateInvariantViolation("remove_missing_pool")
	} else if executorMap, ok := poolMap.Get(executor); !ok || executorMap == nil {
		recordAggregateInvariantViolation("remove_missing_executor")
	} else if executorMap, ok := subQueuePriority(executorMap, queue, pc, req); ok {
		if executorMap.Len() == 0 {
			poolMap = poolMap.Delete(executor)
		} else {
			poolMap = poolMap.Set(executor, executorMap)
		}
		if poolMap.Len() == 0 {
			a.allocatedByExecutor = a.allocatedByExecutor.Delete(pool)
		} else {
			a.allocatedByExecutor = a.allocatedByExecutor.Set(pool, poolMap)
		}
	}

	if _, ok := a.leasedJobs.Get(job.Id()); !ok {
		recordAggregateInvariantViolation("remove_missing_job")
	} else {
		a.leasedJobs = a.leasedJobs.Delete(job.Id())
	}
}

// getQueuedDemand returns queued demand for the given pool and queue by priority
// class. It does not know about cordoned queues; callers decide whether to query
// a queue at all.
func (a *JobAggregate) getQueuedDemand(pool string, queue string) map[string]internaltypes.ResourceList {
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

// getLeasedDemand returns running demand for the given pool and queue by
// priority class, summed over every executor in the pool.
func (a *JobAggregate) getLeasedDemand(pool string, queue string) map[string]internaltypes.ResourceList {
	demand := map[string]internaltypes.ResourceList{}
	if a == nil || a.allocatedByExecutor == nil {
		return demand
	}
	poolMap, ok := a.allocatedByExecutor.Get(pool)
	if !ok || poolMap == nil {
		return demand
	}
	executorIt := poolMap.Iterator()
	for !executorIt.Done() {
		_, executorMap, _ := executorIt.Next()
		if executorMap == nil {
			continue
		}
		queueMap, ok := executorMap.Get(queue)
		if !ok || queueMap == nil {
			continue
		}
		queueIt := queueMap.Iterator()
		for !queueIt.Done() {
			pc, rl, _ := queueIt.Next()
			demand[pc] = demand[pc].Add(rl)
		}
	}
	return demand
}

// CalculateSchedulingInfo derives the per-pool scheduling information for the
// pool described by currentPool, awayAllocationPools and allPools from the
// aggregate. It is intended to be a drop-in (and much cheaper) replacement for
// scanning every job. includeQueued and includeRunning select which parts to
// derive, allowing queued and running jobs to be rolled out independently.
//
// knownQueues is used to discard jobs whose queue no longer exists, and
// cordonedQueues to exclude queued jobs on cordoned queues from demand, matching
// the legacy calculation.
func (a *JobAggregate) CalculateSchedulingInfo(
	activeExecutorsSet map[string]bool,
	currentPool string,
	awayAllocationPools []string,
	allPools []string,
	knownQueues map[string]bool,
	cordonedQueues map[string]bool,
	includeQueued bool,
	includeRunning bool,
) *SchedulingInfo {
	info := &SchedulingInfo{
		JobsByExecutorId:                     map[string][]*Job{},
		JobsByPool:                           map[string][]*Job{},
		DemandByQueueAndPriorityClass:        map[string]map[string]internaltypes.ResourceList{},
		AllocatedByQueueAndPriorityClass:     map[string]map[string]internaltypes.ResourceList{},
		AwayAllocatedByQueueAndPriorityClass: map[string]map[string]internaltypes.ResourceList{},
		InUsePriorityClasses:                 map[string]bool{},
	}
	if a == nil {
		return info
	}

	allPoolsSet := make(map[string]bool, len(allPools))
	for _, pool := range allPools {
		allPoolsSet[pool] = true
	}
	awayPoolsSet := make(map[string]bool, len(awayAllocationPools))
	for _, pool := range awayAllocationPools {
		awayPoolsSet[pool] = true
	}

	// Leased jobs, grouped by their run's pool, and by executor for pools in allPools.
	if includeRunning && a.leasedJobs != nil {
		jobIt := a.leasedJobs.Iterator()
		for !jobIt.Done() {
			_, job, _ := jobIt.Next()
			run := job.LatestRun()
			if run == nil || !queueKnown(knownQueues, job.Queue()) {
				continue
			}
			pool := run.Pool()
			info.JobsByPool[pool] = append(info.JobsByPool[pool], job)
			if allPoolsSet[pool] {
				executor := run.Executor()
				info.JobsByExecutorId[executor] = append(info.JobsByExecutorId[executor], job)
			}
		}
	}

	// Running demand, allocation and in-use priority classes come from the
	// allocation aggregate. Demand counts every executor, allocation only active
	// ones.
	if includeRunning && a.allocatedByExecutor != nil {
		poolIt := a.allocatedByExecutor.Iterator()
		for !poolIt.Done() {
			pool, executorMap, _ := poolIt.Next()
			if executorMap == nil {
				continue
			}
			homePool := pool == currentPool
			awayPool := awayPoolsSet[pool]
			executorIt := executorMap.Iterator()
			for !executorIt.Done() {
				executor, queueMap, _ := executorIt.Next()
				if queueMap == nil {
					continue
				}
				active := activeExecutorsSet[executor]
				queueIt := queueMap.Iterator()
				for !queueIt.Done() {
					queue, pcMap, _ := queueIt.Next()
					if pcMap == nil || !queueKnown(knownQueues, queue) {
						continue
					}
					pcIt := pcMap.Iterator()
					for !pcIt.Done() {
						pc, rl, _ := pcIt.Next()
						info.InUsePriorityClasses[pc] = true
						if homePool {
							addResource(info.DemandByQueueAndPriorityClass, queue, pc, rl)
						}
						if active {
							if homePool {
								addResource(info.AllocatedByQueueAndPriorityClass, queue, pc, rl)
							} else if awayPool {
								addResource(info.AwayAllocatedByQueueAndPriorityClass, queue, pc, rl)
							}
						}
					}
				}
			}
		}
	}

	// In-use priority classes come from queued jobs eligible for any pool in
	// allPools (cordoned queues still count).
	if includeQueued && a.queuedDemand != nil {
		for _, pool := range allPools {
			poolMap, ok := a.queuedDemand.Get(pool)
			if !ok || poolMap == nil {
				continue
			}
			queueIt := poolMap.Iterator()
			for !queueIt.Done() {
				queue, pcMap, _ := queueIt.Next()
				if pcMap == nil || !queueKnown(knownQueues, queue) {
					continue
				}
				pcIt := pcMap.Iterator()
				for !pcIt.Done() {
					pc, _, _ := pcIt.Next()
					info.InUsePriorityClasses[pc] = true
				}
			}
		}
	}

	// Demand from queued jobs on non-cordoned queues eligible for currentPool.
	if includeQueued && a.queuedDemand != nil {
		if poolMap, ok := a.queuedDemand.Get(currentPool); ok && poolMap != nil {
			queueIt := poolMap.Iterator()
			for !queueIt.Done() {
				queue, pcMap, _ := queueIt.Next()
				if pcMap == nil || !queueKnown(knownQueues, queue) || cordonedQueues[queue] {
					continue
				}
				pcIt := pcMap.Iterator()
				for !pcIt.Done() {
					pc, rl, _ := pcIt.Next()
					addResource(info.DemandByQueueAndPriorityClass, queue, pc, rl)
				}
			}
		}
	}

	return info
}

// addQueuePriority adds req to the priority-class bucket of queue in a
// queue -> priority class -> resources map.
func addQueuePriority(
	m *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]],
	queue string,
	pc string,
	req internaltypes.ResourceList,
) *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]] {
	queueMap, _ := m.Get(queue)
	if queueMap == nil {
		queueMap = immutable.NewMap[string, internaltypes.ResourceList](nil)
	}
	current, _ := queueMap.Get(pc)
	return m.Set(queue, queueMap.Set(pc, current.Add(req)))
}

// subQueuePriority removes req from the priority-class bucket of queue in a
// queue -> priority class -> resources map, pruning empty buckets. The second
// return value is false if the bucket was missing or inconsistent, in which
// case an invariant violation is recorded.
func subQueuePriority(
	m *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]],
	queue string,
	pc string,
	req internaltypes.ResourceList,
) (*immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]], bool) {
	queueMap, ok := m.Get(queue)
	if !ok || queueMap == nil {
		recordAggregateInvariantViolation("remove_missing_queue")
		return m, false
	}
	current, ok := queueMap.Get(pc)
	if !ok {
		recordAggregateInvariantViolation("remove_missing_priority_class")
		return m, false
	}
	remaining := current.Subtract(req)
	if remaining.HasNegativeValues() {
		recordAggregateInvariantViolation("remove_negative_remaining")
		return m, false
	}
	if remaining.AllZero() {
		queueMap = queueMap.Delete(pc)
	} else {
		queueMap = queueMap.Set(pc, remaining)
	}
	if queueMap.Len() == 0 {
		return m.Delete(queue), true
	}
	return m.Set(queue, queueMap), true
}

func addResource(dst map[string]map[string]internaltypes.ResourceList, queue string, pc string, rl internaltypes.ResourceList) {
	byPriorityClass, ok := dst[queue]
	if !ok {
		byPriorityClass = map[string]internaltypes.ResourceList{}
		dst[queue] = byPriorityClass
	}
	byPriorityClass[pc] = byPriorityClass[pc].Add(rl)
}

func queueKnown(knownQueues map[string]bool, queue string) bool {
	if knownQueues == nil {
		return true
	}
	return knownQueues[queue]
}
