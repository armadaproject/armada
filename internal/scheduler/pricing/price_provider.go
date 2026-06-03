package pricing

import (
	"github.com/armadaproject/armada/pkg/bidstore"
)

type PriceProvider interface {
	GetPrice(queue string, band bidstore.PriceBand) (map[string]Bid, bool)
}
