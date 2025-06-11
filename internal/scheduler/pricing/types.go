package pricing

import (
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"time"

	"github.com/armadaproject/armada/pkg/bidstore"
)

type BidPriceProvider interface {
	GetBidPrices(ctx *armadacontext.Context) (BidPriceSnapshot, error)
}

// PriceKey bundles the 4 lookup dimensions into one comparable key.
type PriceKey struct {
	Queue string
	Pool  string
	Band  bidstore.PriceBand
	Phase bidstore.PricingPhase
}

// BidPriceSnapshot holds your timestamped flat lookup map.
type BidPriceSnapshot struct {
	Timestamp time.Time
	Prices    map[PriceKey]float64
}

func (s *BidPriceSnapshot) GetPrice(queue, pool string, band bidstore.PriceBand, phase bidstore.PricingPhase,
) (float64, bool) {
	key := PriceKey{
		Queue: queue,
		Pool:  pool,
		Band:  band,
		Phase: phase,
	}
	amt, ok := s.Prices[key]
	return amt, ok
}
