package repository

import (
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/client/queue"
)

// CachedQueueRepository is an implementation of ReadOnlyQueueRepository that fetches ques periodically and caches them.
// This means the queue information may be slightly out of date but allows us to continue api operations even if the
// queue is unavailable
type CachedQueueRepository struct {
	updateFrequency time.Duration
	underlyingRepo  QueueRepository
	queues          atomic.Pointer[map[string]queue.Queue]
}

func NewCachedQueueRepository(underlyingRepo QueueRepository, updateFrequency time.Duration) *CachedQueueRepository {
	return &CachedQueueRepository{
		updateFrequency: updateFrequency,
		underlyingRepo:  underlyingRepo,
		queues:          atomic.Pointer[map[string]queue.Queue]{},
	}
}

func (c *CachedQueueRepository) Run(ctx *armadacontext.Context) error {
	if err := c.fetchQueues(ctx); err != nil {
		ctx.Warnf("Error fetching queues: %v", err)
	}
	ticker := time.NewTicker(c.updateFrequency)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := c.fetchQueues(ctx); err != nil {
				ctx.Warnf("Error fetching queues: %v", err)
			}
		}
	}
}

func (c *CachedQueueRepository) GetQueue(_ *armadacontext.Context, name string) (queue.Queue, error) {
	queues := *(c.queues.Load())
	if queues == nil {
		return queue.Queue{}, &ErrQueueNotFound{QueueName: name}
	}
	resolvedQueue, ok := queues[name]
	if !ok {
		return queue.Queue{}, &ErrQueueNotFound{QueueName: name}
	}
	return resolvedQueue, nil
}

func (c *CachedQueueRepository) GetAllQueues(_ *armadacontext.Context) ([]queue.Queue, error) {
	queues := c.queues.Load()
	if queues == nil {
		return nil, fmt.Errorf("no queues available")
	}
	return maps.Values(*queues), nil
}

func (c *CachedQueueRepository) fetchQueues(ctx *armadacontext.Context) error {
	start := time.Now()
	queues, err := c.underlyingRepo.GetAllQueues(ctx)
	if err != nil {
		return err
	}
	queuesByName := make(map[string]queue.Queue, len(queues))
	for i := 0; i < len(queues); i++ {
		queuesByName[queues[i].Name] = queues[i]
	}
	c.queues.Store(&queuesByName)
	ctx.Infof("Refreshed Queues in %s", time.Since(start))
	return nil
}
