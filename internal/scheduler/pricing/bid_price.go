package pricing

import (
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/cache"
	"github.com/armadaproject/armada/pkg/bidstore"
)

type BidPriceCache struct {
	cache *cache.GenericCache[BidPriceSnapshot]
}

func NewBidPriceCache(client bidstore.BidRetrieverServiceClient, updateFrequency time.Duration) *BidPriceCache {
	svc := NewExternalBidPriceService(client)
	return &BidPriceCache{
		cache: cache.NewGenericCache(svc.GetBidPrices, updateFrequency),
	}
}

func (p *BidPriceCache) Initialise(ctx *armadacontext.Context) error {
	return p.cache.Initialise(ctx)
}

func (p *BidPriceCache) Run(ctx *armadacontext.Context) error {
	return p.cache.Run(ctx)
}

func (p *BidPriceCache) GetBidPrices(ctx *armadacontext.Context) (BidPriceSnapshot, error) {
	return p.cache.Get(ctx)
}
