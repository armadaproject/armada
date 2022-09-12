package scheduler

import (
	"container/heap"

	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/pkg/errors"
)

// QueuePQ Priority queue of queues. Associated with the scheduler.
type QueuePQ []*QueueItem

// Queue that users can submit jobs to.
type QueueItem struct {
	Name string
	// The scheduler balances resource usage between queues, such that if there are n queues
	// with weights weights[0], ..., weights[n-1], then the share given to the i-th queue is
	// weights[i] / (weights[0] + ... + weights[n]) in expectation.
	Weight float64
	// Total amount of resources claimed by the jobs originating from this queue.
	ClaimedResources map[string]int64
	// Jobs waiting to be scheduled.
	Jobs JobPQ
	// QueuePQ priority. Computed from the current resource usage and weight.
	priority float64
	// QueuePQ index. Maintained by the heap interface methods.
	index int
}

func (pq QueuePQ) Len() int {
	return len(pq)
}

func (pq QueuePQ) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq QueuePQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *QueuePQ) Push(x interface{}) {
	n := len(*pq)
	item, ok := x.(*QueueItem)
	if !ok {
		err := errors.Errorf("tried to push %+v of type %T onto QueuePQ", x, x)
		panic(err)
	}
	item.index = n
	*pq = append(*pq, item)
}

func (pq *QueuePQ) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// Update modifies the priority of an item in the queue.
func (pq *QueuePQ) Update(item *QueueItem, priority float64) {
	item.priority = priority
	heap.Fix(pq, item.index)
}

// Priority queue of jobs. Each queue has such a queue associated with it.
type JobPQ []*JobItem

// Job submitted to a queue.
type JobItem struct {
	Id                string
	jobSchedulingInfo *schedulerobjects.JobSchedulingInfo
	// JobPQ priority.
	priority float64
	// JobPQ index. Maintained by the heap interface methods.
	index int
}

func (pq JobPQ) Len() int {
	return len(pq)
}

func (pq JobPQ) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq JobPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *JobPQ) Push(x interface{}) {
	n := len(*pq)
	item, ok := x.(*JobItem)
	if !ok {
		err := errors.Errorf("tried to push %+v of type %T onto QueuePQ", x, x)
		panic(err)
	}
	item.index = n
	*pq = append(*pq, item)
}

func (pq *JobPQ) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// Update modifies the priority of an item in the queue.
func (pq *JobPQ) Update(item *JobItem, priority float64) {
	item.priority = priority
	heap.Fix(pq, item.index)
}
