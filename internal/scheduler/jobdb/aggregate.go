package jobdb

import (
	"github.com/benbjohnson/immutable"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

// SchedulingInfo is the per-pool running scheduling information derived from the
// JobDb aggregate and the set of leased jobs. It mirrors the running half of
// what the scheduler computes by scanning leased jobs (see
// calculateJobSchedulingInfo in the scheduling package).
type SchedulingInfo struct {
	// Leased jobs by executor, restricted to the pools relevant to the round.
	JobsByExecutorId map[string][]*Job
	// Leased jobs by pool.
	JobsByPool map[string][]*Job
	// Running demand by queue, then priority class, for the current pool.
	DemandByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList
	// Allocation by queue, then priority class, for the current pool on active
	// executors.
	AllocatedByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList
	// Away allocation by queue, then priority class, on active executors.
	AwayAllocatedByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList
	// Priority classes used by any leased job.
	InUsePriorityClasses map[string]bool
}

// JobAggregate maintains an incrementally updated aggregate of the active jobs
// in the JobDb so the scheduler can read queued demand and running demand
// without scanning every job.
type JobAggregate struct {
	// queuedEnabled and runningEnabled gate maintenance of the queued and running
	// aggregates independently. When both are false the aggregate stays empty
	// and no per-job maintenance is done.
	queuedEnabled  bool
	runningEnabled bool
	// Queued demand by pool, then queue, then priority class.
	queuedDemand *immutable.Map[string, *immutable.Map[string, *immutable.Map[string, *aggregateBucket]]]
	// Allocation by pool, then executor, then queue, then priority class. Running
	// demand and in-use priority classes are derived from this.
	allocatedByExecutor *immutable.Map[string, *immutable.Map[string, *immutable.Map[string, *immutable.Map[string, *aggregateBucket]]]]
}

// aggregateBucket is the value of a queue/priority-class bucket: the summed
// resources of the jobs contributing to it and the number of contributing
// jobs. The count matters because jobs may request no resources at all (e.g.
// when respectNodePodLimits is off and minJobResources does not require
// positive requests): such a job must keep its bucket (with all-zero
// resources) alive so derived reads still report its queue, priority class and
// demand. A bucket is pruned only when the last contributing job leaves.
type aggregateBucket struct {
	resources internaltypes.ResourceList
	jobs      int
}

func NewJobAggregate() *JobAggregate {
	return &JobAggregate{
		queuedDemand:        immutable.NewMap[string, *immutable.Map[string, *immutable.Map[string, *aggregateBucket]]](nil),
		allocatedByExecutor: immutable.NewMap[string, *immutable.Map[string, *immutable.Map[string, *immutable.Map[string, *aggregateBucket]]]](nil),
	}
}

// add returns a copy of b with rl added and one more contributing job.
func (b *aggregateBucket) add(rl internaltypes.ResourceList) *aggregateBucket {
	if b == nil {
		return &aggregateBucket{resources: rl, jobs: 1}
	}
	return &aggregateBucket{resources: b.resources.Add(rl), jobs: b.jobs + 1}
}

// subtract returns a copy of b with rl and one contributing job removed. The
// caller prunes the bucket when the returned bucket has no remaining jobs.
func (b *aggregateBucket) subtract(rl internaltypes.ResourceList) *aggregateBucket {
	if b == nil {
		return nil
	}
	return &aggregateBucket{resources: b.resources.Subtract(rl), jobs: b.jobs - 1}
}

// SetEnabled toggles queued and running aggregate maintenance independently.
func (a *JobAggregate) SetEnabled(queued, running bool) {
	a.queuedEnabled = queued
	a.runningEnabled = running
}

// enabled reports whether any aggregate maintenance is enabled.
func (a *JobAggregate) enabled() bool {
	return a.queuedEnabled || a.runningEnabled
}

// Clone returns a copy of the aggregate for use by a write transaction. The copy
// shares the current state; changes to it are not visible until committed.
func (a *JobAggregate) Clone() *JobAggregate {
	if a == nil {
		return NewJobAggregate()
	}
	return &JobAggregate{
		queuedEnabled:       a.queuedEnabled,
		runningEnabled:      a.runningEnabled,
		queuedDemand:        a.queuedDemand,
		allocatedByExecutor: a.allocatedByExecutor,
	}
}

// add incorporates job into the aggregate. Jobs that are in a terminal state are
// ignored. Queued jobs contribute queued demand for every pool they are eligible
// for (when queued maintenance is on); leased (running) jobs contribute running
// demand and allocation for the pool and executor of their current run (when
// running maintenance is on).
func (a *JobAggregate) add(job *Job) {
	if a == nil || job == nil || job.InTerminalState() {
		return
	}
	if job.Queued() {
		if a.queuedEnabled {
			a.addQueued(job)
		}
		return
	}
	if run := job.LatestRun(); run != nil && a.runningEnabled {
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
		if a.queuedEnabled {
			a.removeQueued(job)
		}
		return
	}
	if run := job.LatestRun(); run != nil && a.runningEnabled {
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
			poolMap = immutable.NewMap[string, *immutable.Map[string, *aggregateBucket]](nil)
		}
		queueMap, _ := poolMap.Get(queue)
		if queueMap == nil {
			queueMap = immutable.NewMap[string, *aggregateBucket](nil)
		}
		current, _ := queueMap.Get(pc)
		a.queuedDemand = a.queuedDemand.Set(pool, poolMap.Set(queue, queueMap.Set(pc, current.add(req))))
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
		remaining := current.subtract(req)
		if remaining.resources.HasNegativeValues() {
			recordAggregateInvariantViolation("remove_negative_remaining")
			continue
		}
		if remaining.jobs < 0 {
			recordAggregateInvariantViolation("remove_negative_job_count")
			continue
		}
		if remaining.jobs == 0 {
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

func (a *JobAggregate) addLeased(job *Job, run *JobRun) {
	req := job.AllResourceRequirements()
	queue := job.Queue()
	pc := job.PriorityClassName()
	pool := run.Pool()
	executor := run.Executor()

	poolMap, _ := a.allocatedByExecutor.Get(pool)
	if poolMap == nil {
		poolMap = immutable.NewMap[string, *immutable.Map[string, *immutable.Map[string, *aggregateBucket]]](nil)
	}
	executorMap, _ := poolMap.Get(executor)
	if executorMap == nil {
		executorMap = immutable.NewMap[string, *immutable.Map[string, *aggregateBucket]](nil)
	}
	a.allocatedByExecutor = a.allocatedByExecutor.Set(pool, poolMap.Set(executor, addQueuePriority(executorMap, queue, pc, req)))
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
		return
	}
	executorMap, ok := poolMap.Get(executor)
	if !ok || executorMap == nil {
		recordAggregateInvariantViolation("remove_missing_executor")
		return
	}
	executorMap, ok = subQueuePriority(executorMap, queue, pc, req)
	if !ok {
		return
	}
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
		pc, bucket, _ := queueIt.Next()
		demand[pc] = bucket.resources
	}
	return demand
}

// addQueuePriority adds req to the priority-class bucket of queue in a
// queue -> priority class -> bucket map.
func addQueuePriority(
	m *immutable.Map[string, *immutable.Map[string, *aggregateBucket]],
	queue string,
	pc string,
	req internaltypes.ResourceList,
) *immutable.Map[string, *immutable.Map[string, *aggregateBucket]] {
	queueMap, _ := m.Get(queue)
	if queueMap == nil {
		queueMap = immutable.NewMap[string, *aggregateBucket](nil)
	}
	current, _ := queueMap.Get(pc)
	return m.Set(queue, queueMap.Set(pc, current.add(req)))
}

// subQueuePriority removes req from the priority-class bucket of queue in a
// queue -> priority class -> bucket map, pruning buckets with no remaining
// jobs. The second return value is false if the bucket was missing or
// inconsistent, in which case an invariant violation is recorded.
func subQueuePriority(
	m *immutable.Map[string, *immutable.Map[string, *aggregateBucket]],
	queue string,
	pc string,
	req internaltypes.ResourceList,
) (*immutable.Map[string, *immutable.Map[string, *aggregateBucket]], bool) {
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
	remaining := current.subtract(req)
	if remaining.resources.HasNegativeValues() {
		recordAggregateInvariantViolation("remove_negative_remaining")
		return m, false
	}
	if remaining.jobs < 0 {
		recordAggregateInvariantViolation("remove_negative_job_count")
		return m, false
	}
	if remaining.jobs == 0 {
		queueMap = queueMap.Delete(pc)
	} else {
		queueMap = queueMap.Set(pc, remaining)
	}
	if queueMap.Len() == 0 {
		return m.Delete(queue), true
	}
	return m.Set(queue, queueMap), true
}

// addResource adds rl to the priority-class bucket of queue in a
// queue -> priority class -> resources map.
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
