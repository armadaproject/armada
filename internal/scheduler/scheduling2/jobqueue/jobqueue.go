package jobqueue

import (
	"fmt"
	"github.com/emirpasic/gods/trees/binaryheap"
)

type queue struct {
	name string
	cost float64
}

type JobQueue struct {
	pq           *binaryheap.Heap
	activeQueues map[string]bool
}

func (jq *JobQueue) pop() *queue {
	if jq.pq.Empty() {
		return nil
	}
	value, _ := jq.pq.Pop()
	q := value.(*queue)
	delete(jq.activeQueues, q.name)
	return q
}

func (jq *JobQueue) push(q *queue) {
	if jq.activeQueues[q.name] {
		panic(fmt.Sprintf("Duplicate queue %s added to priority queue", q.name))
	}
	jq.activeQueues[q.name] = true
	jq.pq.Push(q)
}

func (jq *JobQueue) remove() {

}
