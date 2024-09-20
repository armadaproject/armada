package jobiteration

import (
	"iter"

	"github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

type (
	JobIterator        = iter.Seq[*jobdb.Job]
	JobContextIterator = iter.Seq[*context.JobSchedulingContext]
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
