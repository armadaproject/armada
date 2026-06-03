package pricing

import (
	"github.com/armadaproject/armada/pkg/bidstore"
)

// PriceProvider provides bid prices for a given queue and price band.
type PriceProvider interface {
	GetPrice(queue string, band bidstore.PriceBand) (map[string]Bid, bool)
}
