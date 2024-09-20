package jobiteration

import (
	"github.com/armadaproject/armada/internal/common/iter"
	"github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

type JobContextRepositoryAdapter struct {
	JobRepository
}

func (j *JobContextRepositoryAdapter) GetJobContextsForQueue(queue string) JobContextIterator {
	return iter.Map(func(j *jobdb.Job) *context.JobSchedulingContext {
		return context.JobSchedulingContextFromJob(j)
	}, j.JobRepository.GetJobsForQueue(queue))
}
