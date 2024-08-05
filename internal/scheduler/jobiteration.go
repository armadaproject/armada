package scheduler

import (
	"sync"

	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

type JobIterator interface {
	Next() (*schedulercontext.JobSchedulingContext, error)
}

type JobRepository interface {
	GetQueueJobIds(queueName string) []string
	GetExistingJobsByIds(ids []string) []*jobdb.Job
}

type InMemoryJobIterator struct {
	i     int
	jctxs []*schedulercontext.JobSchedulingContext
}

func NewInMemoryJobIterator(jctxs []*schedulercontext.JobSchedulingContext) *InMemoryJobIterator {
	return &InMemoryJobIterator{
		jctxs: jctxs,
	}
}

func (it *InMemoryJobIterator) Next() (*schedulercontext.JobSchedulingContext, error) {
	if it.i >= len(it.jctxs) {
		return nil, nil
	}
	v := it.jctxs[it.i]
	it.i++
	return v, nil
}

type InMemoryJobRepository struct {
	jctxsByQueue map[string][]*schedulercontext.JobSchedulingContext
	jctxsById    map[string]*schedulercontext.JobSchedulingContext
	// Protects the above fields.
	mu sync.Mutex
}

func NewInMemoryJobRepository() *InMemoryJobRepository {
	return &InMemoryJobRepository{
		jctxsByQueue: make(map[string][]*schedulercontext.JobSchedulingContext),
		jctxsById:    make(map[string]*schedulercontext.JobSchedulingContext),
	}
}

func (repo *InMemoryJobRepository) EnqueueMany(jctxs []*schedulercontext.JobSchedulingContext) {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	updatedQueues := make(map[string]bool)
	for _, jctx := range jctxs {
		queue := jctx.Job.Queue()
		repo.jctxsByQueue[queue] = append(repo.jctxsByQueue[queue], jctx)
		repo.jctxsById[jctx.Job.Id()] = jctx
		updatedQueues[queue] = true
	}
	for queue := range updatedQueues {
		repo.sortQueue(queue)
	}
}

// sortQueue sorts jobs in a specified queue by the order in which they should be scheduled.
func (repo *InMemoryJobRepository) sortQueue(queue string) {
	slices.SortFunc(repo.jctxsByQueue[queue], func(a, b *schedulercontext.JobSchedulingContext) int {
		return a.Job.SchedulingOrderCompare(b.Job)
	})
}

func (repo *InMemoryJobRepository) GetQueueJobIds(queue string) []string {
	return armadaslices.Map(
		repo.jctxsByQueue[queue],
		func(jctx *schedulercontext.JobSchedulingContext) string {
			return jctx.Job.Id()
		},
	)
}

func (repo *InMemoryJobRepository) GetExistingJobsByIds(jobIds []string) []*jobdb.Job {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	rv := make([]*jobdb.Job, 0, len(jobIds))
	for _, jobId := range jobIds {
		if jctx, ok := repo.jctxsById[jobId]; ok {
			rv = append(rv, jctx.Job)
		}
	}
	return rv
}

func (repo *InMemoryJobRepository) GetJobIterator(queue string) JobIterator {
	repo.mu.Lock()
	defer repo.mu.Unlock()
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
