package jobiteration

import (
	"github.com/armadaproject/armada/internal/common/iter"
	"github.com/armadaproject/armada/internal/scheduler/context"
	"slices"
)

// InMemoryJobContextRepository is JobContextRepository that can be created from a slice of JobSchedulingContexts
type InMemoryJobContextRepository struct {
	jobContextsByQueue map[string][]*context.JobSchedulingContext
}

func NewInMemoryJobContextRepository(jobContexts []*context.JobSchedulingContext) *InMemoryJobContextRepository {

	jobContextsByQueue := make(map[string][]*context.JobSchedulingContext)

	for _, jobCtx := range jobContexts {
		queue, ok := jobContextsByQueue[jobCtx.Job.Queue()]
		if !ok {
			queue = []*context.JobSchedulingContext{}
		}
		jobContextsByQueue[jobCtx.Job.Queue()] = append(queue, jobCtx)
	}

	// Sort jobs by the order in which they should be scheduled.
	for _, queue := range jobContextsByQueue {
		slices.SortFunc(queue, func(a, b *context.JobSchedulingContext) int {
			return a.Job.SchedulingOrderCompare(b.Job)
		})
	}

	return &InMemoryJobContextRepository{
		jobContextsByQueue: jobContextsByQueue,
	}
}

func (repo *InMemoryJobContextRepository) GetJobContextsForQueue(queueName string) JobContextIterator {
	queue, ok := repo.jobContextsByQueue[queueName]
	if ok {
		return slices.Values(queue)
	}
	return iter.Empty[*context.JobSchedulingContext]()
}
