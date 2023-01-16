package scheduler

import (
	"context"

	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/pkg/api"
)

type JobRepositoryAdapter struct {
	jobRepository repository.JobRepository
}

func (j *JobRepositoryAdapter) GetJobIterator(ctx context.Context, queue string) (JobIterator[*api.Job], error) {
	return NewQueuedJobsIterator(ctx, queue, j.jobRepository)
}

func (j *JobRepositoryAdapter) TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error) {
	return j.jobRepository.TryLeaseJobs(clusterId, queue, jobs)
}

func NewJobRepositoryAdapter(jobRepo repository.JobRepository) *JobRepositoryAdapter {
	return &JobRepositoryAdapter{jobRepository: jobRepo}
}
