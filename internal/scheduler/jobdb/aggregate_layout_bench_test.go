package jobdb

import (
	"fmt"
	"testing"

	"github.com/benbjohnson/immutable"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

// BenchmarkAggregateLayout compares the production flat-key aggregate layout
// against a pool-nested layout for add, remove and read. It documents the
// decision to use the flat layout (faster writes, ~2x fewer allocations).
func BenchmarkAggregateLayout(b *testing.B) {
	const (
		numPools        = 4
		numQueues       = 100
		numJobsPerQueue = 1000
	)
	jobDb := NewTestJobDb()
	jobs := make([]*Job, 0, numQueues*numJobsPerQueue)
	for q := 0; q < numQueues; q++ {
		queue := fmt.Sprintf("queue-%d", q)
		for j := 0; j < numJobsPerQueue; j++ {
			pools := []string{fmt.Sprintf("pool-%d", (q+j)%numPools)}
			job := newAggregateTestJob(b, jobDb, fmt.Sprintf("job-%d-%d", q, j), queue, true, pools, 1)
			jobs = append(jobs, job)
		}
	}
	knownQueues := make(map[string]bool, numQueues)
	for q := 0; q < numQueues; q++ {
		knownQueues[fmt.Sprintf("queue-%d", q)] = true
	}

	b.Run("layout=flat/add", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			a := NewJobAggregate()
			for _, job := range jobs {
				a.add(job)
			}
		}
	})

	b.Run("layout=nested/add", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			a := newNestedAggregate()
			for _, job := range jobs {
				a.add(job)
			}
		}
	})

	b.Run("layout=flat/remove", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			a := NewJobAggregate()
			for _, job := range jobs {
				a.add(job)
			}
			b.StartTimer()
			for _, job := range jobs {
				a.remove(job)
			}
		}
	})

	b.Run("layout=nested/remove", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			a := newNestedAggregate()
			for _, job := range jobs {
				a.add(job)
			}
			b.StartTimer()
			for _, job := range jobs {
				a.remove(job)
			}
		}
	})

	flat := NewJobAggregate()
	nested := newNestedAggregate()
	for _, job := range jobs {
		flat.add(job)
		nested.add(job)
	}

	b.Run("layout=flat/read", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_ = flat.getQueuedDemand("pool-0", knownQueues, nil)
		}
	})

	b.Run("layout=nested/read", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_ = nested.getQueuedDemand("pool-0", knownQueues, nil)
		}
	})
}

// nestedAggregate is a benchmark-only candidate layout: pool -> queue -> pc.
type nestedAggregate struct {
	byPool *immutable.Map[string, *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]]]
}

func newNestedAggregate() *nestedAggregate {
	return &nestedAggregate{
		byPool: immutable.NewMap[string, *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]]](nil),
	}
}

func (a *nestedAggregate) add(job *Job) {
	if job == nil || job.InTerminalState() || !job.Queued() {
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

func (a *nestedAggregate) remove(job *Job) {
	if job == nil || job.InTerminalState() || !job.Queued() {
		return
	}
	req := job.AllResourceRequirements()
	queue := job.Queue()
	pc := job.PriorityClassName()
	forEachDistinctPool(job.pools, func(pool string) {
		poolMap, ok := a.byPool.Get(pool)
		if !ok || poolMap == nil {
			return
		}
		queueMap, ok := poolMap.Get(queue)
		if !ok || queueMap == nil {
			return
		}
		current, ok := queueMap.Get(pc)
		if !ok {
			return
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
	})
}

func (a *nestedAggregate) getQueuedDemand(pool string, knownQueues, cordonedQueues map[string]bool) map[string]map[string]internaltypes.ResourceList {
	demand := map[string]map[string]internaltypes.ResourceList{}
	poolMap, ok := a.byPool.Get(pool)
	if !ok || poolMap == nil {
		return demand
	}
	queueIt := poolMap.Iterator()
	for !queueIt.Done() {
		queue, queueMap, _ := queueIt.Next()
		if queueMap == nil || cordonedQueues[queue] || !queueKnown(knownQueues, queue) {
			continue
		}
		byPriorityClass, ok := demand[queue]
		if !ok {
			byPriorityClass = map[string]internaltypes.ResourceList{}
			demand[queue] = byPriorityClass
		}
		pcIt := queueMap.Iterator()
		for !pcIt.Done() {
			pc, rl, _ := pcIt.Next()
			byPriorityClass[pc] = byPriorityClass[pc].Add(rl)
		}
	}
	return demand
}
