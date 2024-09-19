package jobiteration

import (
	"github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

type JobContextIterator interface {
	Next() (*context.JobSchedulingContext, bool)
}

type fish struct {
	jobIterator jobdb.JobIterator
}

func (f *fish) Next() (*context.JobSchedulingContext, bool) {
	job, ok := f.jobIterator.Next()
	if !ok {
		return nil, false
	}
	return context.JobSchedulingContextFromJob(job), true
}
