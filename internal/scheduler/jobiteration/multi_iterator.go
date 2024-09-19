package jobiteration

import (
	"github.com/armadaproject/armada/internal/scheduler/context"
)

// MultiJobsIterator chains several JobIterators together in the order provided.
type MultiJobsIterator struct {
	i   int
	its []JobContextIterator
}

func NewMultiJobsIterator(its ...JobContextIterator) *MultiJobsIterator {
	return &MultiJobsIterator{
		its: its,
	}
}

func (it *MultiJobsIterator) Next() (*context.JobSchedulingContext, bool) {
	if it.i >= len(it.its) {
		return nil, false
	}
	v, ok := it.its[it.i].Next()
	if ok {
		return v, true
	}
	it.i++
	return it.Next()
}
