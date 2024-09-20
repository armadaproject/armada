package jobiteration

import (
	"iter"

	"github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

// JobRepository is a source of jobs
type JobRepository interface {
	GetById(id string) *jobdb.Job
	GetJobsForQueue(queue string) iter.Seq[*jobdb.Job]
}

// JobContextRepository is a source of job contexts
type JobContextRepository interface {
	GetJobContextsForQueue(queue string) iter.Seq[*context.JobSchedulingContext]
}
