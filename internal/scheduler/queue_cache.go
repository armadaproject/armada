package scheduler

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

// QueueCache is an in-memory cache of available queues
type QueueCache interface {

	// Get returns all available queues
	Get(ctx *armadacontext.Context) ([]queue.Queue, error)
}

// ApiQueueCache is an implementation of QueueCache that fetches queues from the Armada API.
// We cache the queues in memory so that we can continue scheduling even if the API is unavailable
type ApiQueueCache struct {
	updateFrequency time.Duration
	apiClient       api.SubmitClient
	queues          atomic.Pointer[[]queue.Queue]
}

func NewQueueCache(apiClient api.SubmitClient, updateFrequency time.Duration) *ApiQueueCache {
	return &ApiQueueCache{
		updateFrequency: updateFrequency,
		apiClient:       apiClient,
		queues:          atomic.Pointer[[]queue.Queue]{},
	}
}

func (c *ApiQueueCache) Run(ctx *armadacontext.Context) error {
	c.fetchQueues(ctx)
	ticker := time.NewTicker(c.updateFrequency)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			queues, err := c.fetchQueues(ctx)
			if err == nil {
				c.queues.Store(&queues)
			} else {
				ctx.Warnf("Error fetching queues: %v", err)
			}
		}
	}
}

func (c *ApiQueueCache) Get(ctx *armadacontext.Context) ([]queue.Queue, error) {
	queues := c.queues.Load()
	if queues == nil {
		return nil, fmt.Errorf("no queues available")
	}
	return *queues, nil
}

func (c *ApiQueueCache) fetchQueues(ctx *armadacontext.Context) ([]queue.Queue, error) {
	stream, err := c.apiClient.GetQueues(ctx, &api.StreamingQueueGetRequest{})
	if err != nil {
		return nil, err
	}
	queues := make([]queue.Queue, 0)
	for {
		msg, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		switch msg.GetEvent().(type) {
		case *api.StreamingQueueMessage_Queue:
			q, err := queue.NewQueue(msg.GetQueue())
			if err != nil {
				return nil, err
			}
			queues = append(queues, q)
		case *api.StreamingQueueMessage_End:
			return queues, nil
		default:
			return nil, fmt.Errorf("unknown event of type %T", msg.GetEvent())
		}
	}
}
