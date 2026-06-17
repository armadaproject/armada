package scheduling

import (
	"container/heap"
	"maps"
	"time"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

func NewShortJobPenalty(cutoffs map[string]time.Duration) *ShortJobPenalty {
	return &ShortJobPenalty{
		cutoffs: cutoffs,
		byId:    map[string]*penaltyEntry{},
		expiry:  &entryHeap{},
		sums:    map[string]map[string]internaltypes.ResourceList{},
	}
}

func (sjp *ShortJobPenalty) SetNow(now time.Time) {
	if sjp == nil {
		return
	}
	sjp.mu.Lock()
	defer sjp.mu.Unlock()
	sjp.now = now
}

func (sjp *ShortJobPenalty) shouldApplyPenalty(job *jobdb.Job) bool {
	if sjp == nil || sjp.now.IsZero() {
		return false
	}

	if !job.InTerminalState() {
		return false
	}

	jobRun := job.LatestRun()
	if jobRun == nil {
		return false
	}

	if jobRun.Preempted() || jobRun.PreemptRequested() || jobRun.PreemptedTime() != nil {
		return false
	}

	jobStart := jobRun.RunningTime()
	if jobStart == nil {
		return false
	}

	return sjp.now.Sub(*jobStart) < sjp.cutoffs[jobRun.Pool()]
}

// ReportFinishedJob applies a terminal short job's resources to its (pool, queue),
// once. Non-terminal and duplicate jobs are ignored.
func (sjp *ShortJobPenalty) ReportFinishedJob(job *jobdb.Job) {
	if sjp == nil {
		return
	}
	sjp.mu.Lock()
	defer sjp.mu.Unlock()
	sjp.expireUpTo(sjp.now)

	if _, alreadyCounted := sjp.byId[job.Id()]; alreadyCounted {
		return
	}
	if !sjp.shouldApplyPenalty(job) {
		return
	}

	run := job.LatestRun()
	pool := run.Pool()
	queue := job.Queue()
	resources := job.AllResourceRequirements()
	deadline := run.RunningTime().Add(sjp.cutoffs[pool])

	e := &penaltyEntry{
		jobId:     job.Id(),
		pool:      pool,
		queue:     queue,
		resources: resources,
		deadline:  deadline,
	}
	sjp.byId[job.Id()] = e
	heap.Push(sjp.expiry, e)
	sjp.addToSums(pool, queue, resources)
}

func (sjp *ShortJobPenalty) GetPenaltiesForPool(pool string) map[string]internaltypes.ResourceList {
	if sjp == nil {
		return nil
	}
	sjp.mu.Lock()
	defer sjp.mu.Unlock()
	sjp.expireUpTo(sjp.now)

	poolSums := sjp.sums[pool]
	if len(poolSums) == 0 {
		return nil
	}
	out := make(map[string]internaltypes.ResourceList, len(poolSums))
	maps.Copy(out, poolSums)
	return out
}

// expireUpTo pops every entry whose deadline is at or before now,
// subtracting its penalty contribution
func (sjp *ShortJobPenalty) expireUpTo(now time.Time) {
	for sjp.expiry.Len() > 0 && !sjp.expiry.peek().deadline.After(now) {
		e := heap.Pop(sjp.expiry).(*penaltyEntry)
		sjp.subtractFromSums(e.pool, e.queue, e.resources)
		delete(sjp.byId, e.jobId)
	}
}

func (sjp *ShortJobPenalty) addToSums(pool, queue string, resources internaltypes.ResourceList) {
	queueSums, ok := sjp.sums[pool]
	if !ok {
		queueSums = map[string]internaltypes.ResourceList{}
		sjp.sums[pool] = queueSums
	}
	queueSums[queue] = queueSums[queue].Add(resources)
}

func (sjp *ShortJobPenalty) subtractFromSums(pool, queue string, resources internaltypes.ResourceList) {
	queueSums := sjp.sums[pool]
	remaining := queueSums[queue].Subtract(resources)
	if remaining.AllZero() {
		delete(queueSums, queue)
		if len(queueSums) == 0 {
			delete(sjp.sums, pool)
		}
		return
	}
	queueSums[queue] = remaining
}
