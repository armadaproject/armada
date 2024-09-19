package scheduler

import (
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

type JobIterator interface {
	Next() (*jobdb.Job, error)
}

type JobRepository interface {
	GetById(id string) *jobdb.Job
	JobsForQueue(queue string) JobIterator
}

type InMemoryJobRepository struct {
	jctxsByQueue map[string][]*schedulercontext.JobSchedulingContext
	jctxsById    map[string]*schedulercontext.JobSchedulingContext
}

func NewInMemoryJobRepository(jctxs []*schedulercontext.JobSchedulingContext) *InMemoryJobRepository {

	jctxsByQueue := make(map[string][]*schedulercontext.JobSchedulingContext)
	jctxsById := make(map[string]*schedulercontext.JobSchedulingContext, len(jctxs))

	for _, jctx := range jctxs {
		queue := jctx.Job.Queue()
		jctxsByQueue[queue] = append(jctxsByQueue[queue], jctx)
		jctxsById[jctx.Job.Id()] = jctx
	}
	for _, jobCtxs := range jctxsByQueue {
		slices.SortFunc(jobCtxs, func(a, b *schedulercontext.JobSchedulingContext) int {
			return a.Job.SchedulingOrderCompare(b.Job)
		})
	}

	return &InMemoryJobRepository{
		jctxsByQueue: jctxsByQueue,
		jctxsById:    jctxsById,
	}
}

func (repo *InMemoryJobRepository) GetQueueJobIds(queue string) []string {
	return armadaslices.Map(
		repo.jctxsByQueue[queue],
		func(jctx *schedulercontext.JobSchedulingContext) string {
			return jctx.Job.Id()
		},
	)
}

func (repo *InMemoryJobRepository) GetById(jobId string) *jobdb.Job {
	if jctx, ok := repo.jctxsById[jobId]; ok {
		return jctx.Job
	}
	return nil
}

func (repo *InMemoryJobRepository) GetJobIterator(queue string) JobIterator {
	return NewInMemoryJobIterator(slices.Clone(repo.jctxsByQueue[queue]))
}

// QueuedJobsIterator is an iterator over all jobs in a queue.
type QueuedJobsIterator struct {
	repo   JobRepository
	jobIds []string
	idx    int
	ctx    *armadacontext.Context
}

func NewQueuedJobsIterator(ctx *armadacontext.Context, queue string, repo JobRepository) *QueuedJobsIterator {
	return &QueuedJobsIterator{
		jobIds: repo.GetQueueJobIds(queue),
		repo:   repo,
		ctx:    ctx,
	}
}

func (it *QueuedJobsIterator) Next() (*schedulercontext.JobSchedulingContext, error) {
	select {
	case <-it.ctx.Done():
		return nil, it.ctx.Err()
	default:
		if it.idx >= len(it.jobIds) {
			return nil, nil
		}
		job := it.repo.GetExistingJobsByIds([]string{it.jobIds[it.idx]})
		it.idx++
		return schedulercontext.JobSchedulingContextFromJob(job[0]), nil
	}
}

// MultiJobsIterator chains several JobIterators together in the order provided.
type MultiJobsIterator struct {
	i   int
	its []JobIterator
}

func NewMultiJobsIterator(its ...JobIterator) *MultiJobsIterator {
	return &MultiJobsIterator{
		its: its,
	}
}

func (it *MultiJobsIterator) Next() (*schedulercontext.JobSchedulingContext, error) {
	if it.i >= len(it.its) {
		return nil, nil
	}
	v, err := it.its[it.i].Next()
	if err != nil {
		return nil, err
	}
	if v == nil {
		it.i++
		return it.Next()
	} else {
		return v, err
	}
}
