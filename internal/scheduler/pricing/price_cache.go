package pricing

import (
	"sync"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/bidstore"
)

// PriceCache wraps a BidPriceProvider, caching the most recent snapshot.
type PriceCache struct {
	priceProvider  BidPriceProvider
	snapshotLock   sync.RWMutex
	latestSnapshot *BidPriceSnapshot
}

func NewPriceCache(priceProvider BidPriceProvider) *PriceCache {
	return &PriceCache{priceProvider: priceProvider}
}

// UpdatePrices fetches fresh prices from the underlying provider and caches the result.
func (c *PriceCache) UpdatePrices(ctx *armadacontext.Context) error {
	snapshot, err := c.priceProvider.GetBidPrices(ctx)
	if err != nil {
		return err
	}
	c.snapshotLock.Lock()
	defer c.snapshotLock.Unlock()
	c.latestSnapshot = &snapshot
	return nil
}

// GetPrice implements jobdb.PriceProvider, returning the current cached price for the
// given queue and price band.
func (c *PriceCache) GetPrice(queue string, band bidstore.PriceBand) (map[string]Bid, bool) {
	c.snapshotLock.RLock()
	defer c.snapshotLock.RUnlock()
	if c.latestSnapshot == nil {
		return nil, false
	}
	return c.latestSnapshot.GetPrice(queue, band)
}

// CurrentSnapshot returns the cached snapshot, or nil if UpdatePrices has not yet been called.
func (c *PriceCache) CurrentSnapshot() *BidPriceSnapshot {
	c.snapshotLock.RLock()
	defer c.snapshotLock.RUnlock()
	return c.latestSnapshot
}
