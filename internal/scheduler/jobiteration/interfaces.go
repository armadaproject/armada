package jobiteration

import (
	"github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/iter"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

type (
	JobIterator        = iter.Iterator[*jobdb.Job]
	JobContextIterator = iter.Iterator[*context.JobSchedulingContext]
)

// JobRepository is a source of jobs
type JobRepository interface {
	GetById(id string) *jobdb.Job
	GetJobsForQueue(queue string) JobIterator
}

// JobContextRepository is a source of job contexts
type JobContextRepository interface {
	GetJobContextsForQueue(queue string) JobContextIterator
}
