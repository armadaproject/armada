package pricing

import (
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/bidstore"
)

type BidPriceProvider interface {
	GetBidPrices(ctx *armadacontext.Context) (BidPriceSnapshot, error)
}

type PriceKey struct {
	Queue string
	Band  bidstore.PriceBand
}

type BidPriceSnapshot struct {
	Timestamp time.Time
	Bids      map[PriceKey]map[string]Bid
}

type Bid struct {
	RunningBid float64
	QueuedBid  float64
}

func (s *BidPriceSnapshot) GetPrice(queue string, band bidstore.PriceBand) (map[string]Bid, bool) {
	key := PriceKey{
		Queue: queue,
		Band:  band,
	}
	price, ok := s.Bids[key]
	return price, ok
}
