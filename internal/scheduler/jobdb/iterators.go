package jobdb

import (
	"fmt"
	"math"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
)

// JobQueueIterator is an iterator over all jobs in a given queue.
// Jobs are sorted first by per-queue priority, and secondly by submission time.
type JobQueueIterator struct {
	queue string
	it    memdb.ResultIterator
}

func NewJobQueueIterator(txn *memdb.Txn, queue string) (*JobQueueIterator, error) {
	var minPriority uint32 = 0
	minTimestamp := -math.MaxInt64
	it, err := txn.LowerBound(jobsTable, orderIndex, queue, true, minPriority, minTimestamp)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &JobQueueIterator{
		queue: queue,
		it:    it,
	}, nil
}

// WatchCh is needed to implement the memdb.ResultIterator interface but is not needed for our use case
func (it *JobQueueIterator) WatchCh() <-chan struct{} {
	panic("not implemented")
}

// NextJobItem returns the next Job or nil if the end of the iterator has been reached
func (it *JobQueueIterator) NextJobItem() *Job {
	jobItem := nextJobItem(it.it)
	if jobItem == nil {
		return nil
	}
	if jobItem.queue != it.queue {
		// The index is sorted by queue first.
		// So we've seen all jobs in this queue when this comparison fails.
		return nil
	}
	return jobItem
}

// Next is needed to implement the memdb.ResultIterator interface.  External callers should use NextJobItem which
// provides a typesafe mechanism for getting the next Job
func (it *JobQueueIterator) Next() interface{} {
	return it.NextJobItem()
}

// LeasedJobsIterator is an iterator over all queued jobs
type LeasedJobsIterator struct {
	it memdb.ResultIterator
}

func NewLeasedJobsIterator(txn *memdb.Txn) (*LeasedJobsIterator, error) {
	it, err := txn.Get(jobsTable, queuedJobsIndex, false)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &LeasedJobsIterator{
		it: it,
	}, nil
}

// WatchCh is needed to implement the memdb.ResultIterator interface but is not needed for our use case
func (it *LeasedJobsIterator) WatchCh() <-chan struct{} {
	panic("not implemented")
}

// NextJobItem returns the next Job or nil if the end of the iterator has been reached
func (it *LeasedJobsIterator) NextJobItem() *Job {
	return nextJobItem(it.it)
}

// Next is needed to implement the memdb.ResultIterator interface.  External callers should use NextJobItem which
// provides a typesafe mechanism for getting the next Job
func (it *LeasedJobsIterator) Next() interface{} {
	return it.NextJobItem()
}

func nextJobItem(it memdb.ResultIterator) *Job {
	obj := it.Next()
	if obj == nil {
		return nil
	}
	jobItem, ok := obj.(*Job)
	if !ok {
		panic(fmt.Sprintf("expected *Job, but got %T", obj))
	}
	return jobItem
}
