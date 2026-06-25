package pricing

import (
	"maps"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/pkg/bidstore"
)

const NonPreemptibleRunningPrice = 1_000_000

type BidPriceProvider interface {
	GetBidPrices(ctx *armadacontext.Context) (BidPriceSnapshot, error)
}

type PriceKey struct {
	Queue string
	Band  bidstore.PriceBand
}

type BidPriceSnapshot struct {
	// Id uniquely identifies this snapshot.
	// Two snapshots with the same non-empty Id are guaranteed to hold identical Bids and ResourceUnits.
	// An empty Id means "unidentified" and should never be treated as equal to another.
	Id            string
	Timestamp     time.Time
	Bids          map[PriceKey]map[string]Bid
	ResourceUnits map[string]internaltypes.ResourceList
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

// ChangedPriceKeys returns the set of PriceKeys whose bids differ between the two snapshots.
// A key is considered changed if it was added, removed, or if its per-pool bids differ.
func (s *BidPriceSnapshot) ChangedPriceKeys(comparisonSnapshot *BidPriceSnapshot) map[PriceKey]bool {
	changed := make(map[PriceKey]bool)
	var comparisonBids map[PriceKey]map[string]Bid
	if comparisonSnapshot != nil {
		comparisonBids = comparisonSnapshot.Bids
	}
	for key, bids := range s.Bids {
		if !maps.Equal(comparisonBids[key], bids) {
			changed[key] = true
		}
	}
	// Report keys that were removed entirely (present previously, gone now).
	for key := range comparisonBids {
		if _, ok := s.Bids[key]; !ok {
			changed[key] = true
		}
	}
	return changed
}
