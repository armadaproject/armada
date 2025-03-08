package scheduling

import (
	"sync"

	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type JobContextIterator interface {
	Next() (*schedulercontext.JobSchedulingContext, error)
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
	currentPool  string
	sortOrder    func(a, b *jobdb.Job) int
	// Protects the above fields.
	mu sync.Mutex
}

func NewInMemoryJobRepository(pool string, sortOrder func(a, b *jobdb.Job) int) *InMemoryJobRepository {
	return &InMemoryJobRepository{
		currentPool:  pool,
		jctxsByQueue: make(map[string][]*schedulercontext.JobSchedulingContext),
		jctxsById:    make(map[string]*schedulercontext.JobSchedulingContext),
		sortOrder:    sortOrder,
	}
}

func (repo *InMemoryJobRepository) EnqueueMany(jctxs []*schedulercontext.JobSchedulingContext) {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	updatedQueues := make(map[string]bool)
	for _, jctx := range jctxs {
		queue := jctx.Job.Queue()
		if jctx.Job.LatestRun() != nil && jctx.Job.LatestRun().Pool() != repo.currentPool {
			queue = schedulercontext.CalculateAwayQueueName(jctx.Job.Queue())
		}
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
		return repo.sortOrder(a.Job, b.Job)
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

func (repo *InMemoryJobRepository) GetJobIterator(queue string) JobContextIterator {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	return NewInMemoryJobIterator(slices.Clone(repo.jctxsByQueue[queue]))
}

// QueuedJobsIterator is an iterator over all jobs in a queue.
type QueuedJobsIterator struct {
	jobIter jobdb.JobIterator
	pool    string
	ctx     *armadacontext.Context
}

func NewQueuedJobsIterator(ctx *armadacontext.Context, queue string, pool string, repo jobdb.JobRepository, order jobdb.JobSortOrder) *QueuedJobsIterator {
	return &QueuedJobsIterator{
		jobIter: repo.QueuedJobs(queue, order),
		pool:    pool,
		ctx:     ctx,
	}
}

func (it *QueuedJobsIterator) Next() (*schedulercontext.JobSchedulingContext, error) {
	for {
		select {
		case <-it.ctx.Done():
			return nil, it.ctx.Err()
		default:
			job, _ := it.jobIter.Next()
			if job == nil {
				return nil, nil
			}
			if slices.Contains(job.Pools(), it.pool) {
				return schedulercontext.JobSchedulingContextFromJob(job), nil
			}
		}
	}
}

// MultiJobsIterator chains several JobIterators together in the order provided.
type MultiJobsIterator struct {
	i   int
	its []JobContextIterator
}

func NewMultiJobsIterator(its ...JobContextIterator) *MultiJobsIterator {
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

// MarketDrivenMultiJobsIterator combines two iterators by price
type MarketDrivenMultiJobsIterator struct {
	it1 JobContextIterator
	it2 JobContextIterator

	// TODO: ideally we add peek() to JobContextIterator and remove these
	it1Value *schedulercontext.JobSchedulingContext
	it2Value *schedulercontext.JobSchedulingContext
}

func NewMarketDrivenMultiJobsIterator(it1, it2 JobContextIterator) *MarketDrivenMultiJobsIterator {
	return &MarketDrivenMultiJobsIterator{
		it1: it1,
		it2: it2,
	}
}

func (it *MarketDrivenMultiJobsIterator) Next() (*schedulercontext.JobSchedulingContext, error) {
	if it.it1Value == nil {
		j, err := it.it1.Next()
		if err != nil {
			return nil, err
		}
		it.it1Value = j
	}

	if it.it2Value == nil {
		j, err := it.it2.Next()
		if err != nil {
			return nil, err
		}
		it.it2Value = j
	}

	j1 := it.it1Value
	j2 := it.it2Value
	// Both iterators active.
	if it.it1Value != nil && j2 != nil {
		if (jobdb.MarketSchedulingOrderCompare(j1.Job, j2.Job)) < 0 {
			it.it1Value = nil
			return j1, nil
		} else {
			it.it2Value = nil
			return j2, nil
		}
	}

	// Only first iterator has job
	if j1 != nil {
		it.it1Value = nil
		return j1, nil
	}

	// Only second iterator has job
	if j2 != nil {
		it.it2Value = nil
		return j2, nil
	}

	// If we get to here then both iterators exhausted
	return nil, nil
}
