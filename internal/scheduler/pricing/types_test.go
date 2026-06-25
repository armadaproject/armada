package pricing

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/pkg/bidstore"
)

func TestChangedPriceKeys(t *testing.T) {
	keyA := PriceKey{Queue: "queueA", Band: bidstore.PriceBand_PRICE_BAND_A}
	keyB := PriceKey{Queue: "queueB", Band: bidstore.PriceBand_PRICE_BAND_B}

	bid := func(q, r float64) map[string]Bid {
		return map[string]Bid{"pool1": {QueuedBid: q, RunningBid: r}}
	}

	tests := map[string]struct {
		comparison *BidPriceSnapshot
		current    *BidPriceSnapshot
		expected   map[PriceKey]bool
	}{
		"nil - everything changed": {
			comparison: nil,
			current:    &BidPriceSnapshot{Bids: map[PriceKey]map[string]Bid{keyA: bid(1, 1)}},
			expected:   map[PriceKey]bool{keyA: true},
		},
		"identical - nothing changed": {
			comparison: &BidPriceSnapshot{Bids: map[PriceKey]map[string]Bid{keyA: bid(1, 1)}},
			current:    &BidPriceSnapshot{Bids: map[PriceKey]map[string]Bid{keyA: bid(1, 1)}},
			expected:   map[PriceKey]bool{},
		},
		"changed value": {
			comparison: &BidPriceSnapshot{Bids: map[PriceKey]map[string]Bid{keyA: bid(1, 1)}},
			current:    &BidPriceSnapshot{Bids: map[PriceKey]map[string]Bid{keyA: bid(2, 2)}},
			expected:   map[PriceKey]bool{keyA: true},
		},
		"added key": {
			comparison: &BidPriceSnapshot{Bids: map[PriceKey]map[string]Bid{keyA: bid(1, 1)}},
			current:    &BidPriceSnapshot{Bids: map[PriceKey]map[string]Bid{keyA: bid(1, 1), keyB: bid(1, 1)}},
			expected:   map[PriceKey]bool{keyB: true},
		},
		"removed key is reported as changed": {
			comparison: &BidPriceSnapshot{Bids: map[PriceKey]map[string]Bid{keyA: bid(1, 1), keyB: bid(1, 1)}},
			current:    &BidPriceSnapshot{Bids: map[PriceKey]map[string]Bid{keyA: bid(1, 1)}},
			expected:   map[PriceKey]bool{keyB: true},
		},
		"added pool within a key": {
			comparison: &BidPriceSnapshot{Bids: map[PriceKey]map[string]Bid{keyA: bid(1, 1)}},
			current: &BidPriceSnapshot{Bids: map[PriceKey]map[string]Bid{keyA: {
				"pool1": {QueuedBid: 1, RunningBid: 1},
				"pool2": {QueuedBid: 1, RunningBid: 1},
			}}},
			expected: map[PriceKey]bool{keyA: true},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.current.ChangedPriceKeys(tc.comparison))
		})
	}
}
