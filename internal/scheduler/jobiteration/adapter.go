package jobiteration

import (
	"iter"

	"github.com/armadaproject/armada/internal/common/xiter"
	"github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

type JobContextRepositoryAdapter struct {
	JobRepository
}

func (j *JobContextRepositoryAdapter) GetJobContextsForQueue(queue string) iter.Seq[*context.JobSchedulingContext] {
	return xiter.Map(func(j *jobdb.Job) *context.JobSchedulingContext {
		return context.JobSchedulingContextFromJob(j)
	}, j.JobRepository.GetJobsForQueue(queue))
}
