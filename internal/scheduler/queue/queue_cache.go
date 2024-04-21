package queue

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/api"
)

// QueueCache is an in-memory cache of available queues
type QueueCache interface {
	// Get returns all available queues
	GetAll(ctx *armadacontext.Context) ([]*api.Queue, error)
}

// ApiQueueCache is an implementation of QueueCache that fetches queues from the Armada API.
// We cache the queues in memory so that we can continue scheduling even if the API is unavailable
type ApiQueueCache struct {
	updateFrequency time.Duration
	apiClient       api.SubmitClient
	queues          atomic.Pointer[[]*api.Queue]
}

func NewQueueCache(apiClient api.SubmitClient, updateFrequency time.Duration) *ApiQueueCache {
	return &ApiQueueCache{
		updateFrequency: updateFrequency,
		apiClient:       apiClient,
		queues:          atomic.Pointer[[]*api.Queue]{},
	}
}

func (c *ApiQueueCache) Run(ctx *armadacontext.Context) error {
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

func (c *ApiQueueCache) GetAll(_ *armadacontext.Context) ([]*api.Queue, error) {
	queues := c.queues.Load()
	if queues == nil {
		return nil, fmt.Errorf("no queues available")
	}
	return *queues, nil
}

func (c *ApiQueueCache) fetchQueues(ctx *armadacontext.Context) error {
	stream, err := c.apiClient.GetQueues(ctx, &api.StreamingQueueGetRequest{})
	if err != nil {
		return err
	}
	queues := make([]*api.Queue, 0)
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		switch msg.GetEvent().(type) {
		case *api.StreamingQueueMessage_Queue:
			queues = append(queues, msg.GetQueue())
		case *api.StreamingQueueMessage_End:
			c.queues.Store(&queues)
			return nil
		default:
			fmt.Errorf("unknown event of type %T", msg.GetEvent())
		}
	}
}
