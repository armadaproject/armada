package scheduler

import (
	"sync"

	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
)

type JobIterator interface {
	Next() (*schedulercontext.JobSchedulingContext, error)
}

type JobRepository interface {
	GetQueueJobIds(queueName string) ([]string, error)
	GetExistingJobsByIds(ids []string) ([]interfaces.LegacySchedulerJob, error)
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
		queue := jctx.Job.GetQueue()
		repo.jctxsByQueue[queue] = append(repo.jctxsByQueue[queue], jctx)
		repo.jctxsById[jctx.Job.GetId()] = jctx
		updatedQueues[queue] = true
	}
	for queue := range updatedQueues {
		repo.sortQueue(queue)
	}
}

// sortQueue sorts jobs in a specified queue by the order in which they should be scheduled.
func (repo *InMemoryJobRepository) sortQueue(queue string) {
	slices.SortFunc(repo.jctxsByQueue[queue], func(a, b *schedulercontext.JobSchedulingContext) bool {
		return a.Job.SchedulingOrderCompare(b.Job) == -1
	})
}

// Should only be used in testing.
func (repo *InMemoryJobRepository) GetQueueJobIds(queue string) ([]string, error) {
	return util.Map(
		repo.jctxsByQueue[queue],
		func(jctx *schedulercontext.JobSchedulingContext) string {
			return jctx.Job.GetId()
		},
	), nil
}

func (repo *InMemoryJobRepository) GetExistingJobsByIds(jobIds []string) ([]interfaces.LegacySchedulerJob, error) {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	rv := make([]interfaces.LegacySchedulerJob, 0, len(jobIds))
	for _, jobId := range jobIds {
		if jctx, ok := repo.jctxsById[jobId]; ok {
			rv = append(rv, jctx.Job)
		}
	}
	return rv, nil
}

func (repo *InMemoryJobRepository) GetJobIterator(queue string) JobIterator {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	return NewInMemoryJobIterator(slices.Clone(repo.jctxsByQueue[queue]))
}

// QueuedJobsIterator is an iterator over all jobs in a queue.
// It loads jobs asynchronously in batches from the underlying database.
// This is necessary for good performance when jobs are stored in Redis.
type QueuedJobsIterator struct {
	ctx             *armadacontext.Context
	err             error
	c               chan interfaces.LegacySchedulerJob
	priorityClasses map[string]types.PriorityClass
}

func NewQueuedJobsIterator(ctx *armadacontext.Context, queue string, repo JobRepository, priorityClasses map[string]types.PriorityClass) (*QueuedJobsIterator, error) {
	batchSize := 16
	g, ctx := armadacontext.ErrGroup(ctx)
	it := &QueuedJobsIterator{
		ctx:             ctx,
		c:               make(chan interfaces.LegacySchedulerJob, 2*batchSize), // 2x batchSize to load one batch async.
		priorityClasses: priorityClasses,
	}

	jobIds, err := repo.GetQueueJobIds(queue)
	if err != nil {
		it.err = err
		return nil, err
	}
	g.Go(func() error { return queuedJobsIteratorLoader(ctx, jobIds, it.c, batchSize, repo) })

	return it, nil
}

func (it *QueuedJobsIterator) Next() (*schedulercontext.JobSchedulingContext, error) {
	// Once this function has returned error,
	// it will return this error on every invocation.
	if it.err != nil {
		return nil, it.err
	}

	// Get one job that was loaded asynchronously.
	select {
	case <-it.ctx.Done():
		it.err = it.ctx.Err() // Return an error if called again.
		return nil, it.err
	case job, ok := <-it.c:
		if !ok {
			return nil, nil
		}
		return schedulercontext.JobSchedulingContextFromJob(
			it.priorityClasses,
			job,
			GangIdAndCardinalityFromAnnotations,
		), nil
	}
}

// queuedJobsIteratorLoader loads jobs from Redis lazily.
// Used with QueuedJobsIterator.
func queuedJobsIteratorLoader(
	ctx *armadacontext.Context,
	jobIds []string,
	ch chan interfaces.LegacySchedulerJob,
	batchSize int,
	repo JobRepository,
) error {
	defer close(ch)
	batch := make([]string, batchSize)
	for i, jobId := range jobIds {
		batch[i%len(batch)] = jobId
		if (i+1)%len(batch) == 0 || i == len(jobIds)-1 {
			jobs, err := repo.GetExistingJobsByIds(batch[:i%len(batch)+1])
			if err != nil {
				return err
			}
			for _, job := range jobs {
				if job == nil {
					continue
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ch <- job:
				}
			}
		}
	}
	return nil
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
	if v, err := it.its[it.i].Next(); err != nil {
		return nil, err
	} else if v == nil {
		it.i++
		return it.Next()
	} else {
		return v, nil
	}
}
