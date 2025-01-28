package queue

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/api"
)

// Cache is an in-memory cache of available priority multipliers
type Cache interface {
	// GetAll returns all available priority multipliers
	GetAll(ctx *armadacontext.Context) ([]*api.Queue, error)
}

// ApiCache is an implementation of Cache that fetches priority multipliers from the Priority Multiplier Service.
// We cache the multipliers in memory so that we can continue scheduling even if the API is unavailable
type ApiCache struct {
	updateFrequency time.Duration
	apiClient       api.SubmitClient
	queues          atomic.Pointer[[]*api.Queue]
}

func NewQueueCache(apiClient api.SubmitClient, updateFrequency time.Duration) *ApiCache {
	return &ApiCache{
		updateFrequency: updateFrequency,
		apiClient:       apiClient,
		queues:          atomic.Pointer[[]*api.Queue]{},
	}
}

func (c *ApiCache) Run(ctx *armadacontext.Context) error {
	if err := c.fetchMultipliers(ctx); err != nil {
		ctx.Warnf("Error fetching multipliers: %v", err)
	}
	ticker := time.NewTicker(c.updateFrequency)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := c.fetchMultipliers(ctx); err != nil {
				ctx.Warnf("Error fetching multipliers: %v", err)
			}
		}
	}
}

func (c *ApiCache) GetAll(_ *armadacontext.Context) ([]*api.Queue, error) {
	queues := c.queues.Load()
	if queues == nil {
		return nil, fmt.Errorf("no multipliers available")
	}
	return *queues, nil
}

func (c *ApiCache) fetchMultipliers(ctx *armadacontext.Context) error {
	start := time.Now()
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
			ctx.Infof("Refreshed Queues in %s", time.Since(start))
			return nil
		default:
			return fmt.Errorf("unknown event of type %T", msg.GetEvent())
		}
	}
}
