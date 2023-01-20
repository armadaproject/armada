package scheduler

import (
	"context"

	"github.com/hashicorp/go-memdb"

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

type JobDbAdapter struct {
	txn *memdb.Txn
}

func NewJobDbAdapter(txn *memdb.Txn) *JobDbAdapter {
	return &JobDbAdapter{txn}
}

func (j *JobDbAdapter) GetJobIterator(_ context.Context, queue string) (JobIterator[*SchedulerJob], error) {
	underlyingIter, err := NewJobQueueIterator(j.txn, queue)
	if err != nil {
		return nil, err
	}
	return &jobQueueIteratorAdapter{iter: underlyingIter}, nil
}

func (j *JobDbAdapter) TryLeaseJobs(_ string, _ string, jobs []*SchedulerJob) ([]*SchedulerJob, error) {
	return jobs, nil
}

type jobQueueIteratorAdapter struct {
	iter *JobQueueIterator
}

func (i *jobQueueIteratorAdapter) Next() (*SchedulerJob, error) {
	return i.iter.NextJobItem(), nil
}
