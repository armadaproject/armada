package pricing

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/bidstore"
)

type stubBidPriceProvider struct {
	calls    int
	snapshot BidPriceSnapshot
	err      error
}

func (s *stubBidPriceProvider) GetBidPrices(_ *armadacontext.Context) (BidPriceSnapshot, error) {
	s.calls += 1
	return s.snapshot, s.err
}

func snapshotWith(queue string, band bidstore.PriceBand, bids map[string]Bid) BidPriceSnapshot {
	return BidPriceSnapshot{
		Timestamp: time.Unix(0, 0),
		Bids: map[PriceKey]map[string]Bid{
			{Queue: queue, Band: band}: bids,
		},
	}
}

func TestPriceCache_GetPrice_BeforeUpdateReturnsNotFound(t *testing.T) {
	cache := NewPriceCache(&stubBidPriceProvider{})

	prices, ok := cache.GetPrice("queue-a", bidstore.PriceBand_PRICE_BAND_A)
	assert.False(t, ok)
	assert.Nil(t, prices)
	assert.Nil(t, cache.CurrentSnapshot())
}

func TestPriceCache_GetPrice_ReturnsPriceFromSnapshot(t *testing.T) {
	bids := map[string]Bid{
		"pool-1": {RunningBid: 1.0, QueuedBid: 0.5},
	}
	provider := &stubBidPriceProvider{
		snapshot: snapshotWith("queue-a", bidstore.PriceBand_PRICE_BAND_A, bids),
	}
	cache := NewPriceCache(provider)

	require.NoError(t, cache.UpdatePrices(armadacontext.Background()))

	call1, ok := cache.GetPrice("queue-a", bidstore.PriceBand_PRICE_BAND_A)
	require.True(t, ok)
	assert.Equal(t, bids, call1)
	assert.Equal(t, provider.calls, 1)

	call2, ok := cache.GetPrice("queue-a", bidstore.PriceBand_PRICE_BAND_A)
	require.True(t, ok)
	assert.Equal(t, bids, call2)
	assert.Equal(t, provider.calls, 1)

	call3, ok := cache.GetPrice("queue-a", bidstore.PriceBand_PRICE_BAND_A)
	require.True(t, ok)
	assert.Equal(t, bids, call3)
	assert.Equal(t, provider.calls, 1)
}

func TestPriceCache_GetPrice_ReturnsUpdatedPrice_AfterUpdateCall(t *testing.T) {
	bids := map[string]Bid{
		"pool-1": {RunningBid: 1.0, QueuedBid: 0.5},
	}
	bids2 := map[string]Bid{
		"pool-1": {RunningBid: 2.0, QueuedBid: 1.0},
	}

	// Set initial prices in provider
	provider := &stubBidPriceProvider{
		snapshot: snapshotWith("queue-a", bidstore.PriceBand_PRICE_BAND_A, bids),
	}
	cache := NewPriceCache(provider)

	require.NoError(t, cache.UpdatePrices(armadacontext.Background()))

	price, ok := cache.GetPrice("queue-a", bidstore.PriceBand_PRICE_BAND_A)
	require.True(t, ok)
	assert.Equal(t, bids, price)
	assert.Equal(t, provider.calls, 1)
	// Update price in provider and request cache update it's prices
	provider.snapshot = snapshotWith("queue-a", bidstore.PriceBand_PRICE_BAND_A, bids2)
	require.NoError(t, cache.UpdatePrices(armadacontext.Background()))

	price, ok = cache.GetPrice("queue-a", bidstore.PriceBand_PRICE_BAND_A)
	require.True(t, ok)
	assert.Equal(t, bids2, price)
	assert.Equal(t, provider.calls, 2)
}

func TestPriceCache_UpdatePrices_PopulatesSnapshot(t *testing.T) {
	bids := map[string]Bid{
		"pool-1": {RunningBid: 1.0, QueuedBid: 0.5},
	}
	provider := &stubBidPriceProvider{
		snapshot: snapshotWith("queue-a", bidstore.PriceBand_PRICE_BAND_A, bids),
	}
	cache := NewPriceCache(provider)

	require.NoError(t, cache.UpdatePrices(armadacontext.Background()))

	assert.Equal(t, provider.calls, 1)
	assert.NotNil(t, cache.CurrentSnapshot())
	assert.Equal(t, cache.CurrentSnapshot(), &provider.snapshot)
}

func TestPriceCache_GetPrice_UnknownKeyReturnsNotFound(t *testing.T) {
	provider := &stubBidPriceProvider{
		snapshot: snapshotWith("queue-a", bidstore.PriceBand_PRICE_BAND_A, map[string]Bid{
			"pool-1": {RunningBid: 1.0},
		}),
	}
	cache := NewPriceCache(provider)
	require.NoError(t, cache.UpdatePrices(armadacontext.Background()))

	prices, ok := cache.GetPrice("queue-b", bidstore.PriceBand_PRICE_BAND_A)
	assert.False(t, ok)
	assert.Nil(t, prices)

	prices, ok = cache.GetPrice("queue-a", bidstore.PriceBand_PRICE_BAND_B)
	assert.False(t, ok)
	assert.Nil(t, prices)
}

func TestPriceCache_UpdatePrices_PropagatesError(t *testing.T) {
	wantErr := errors.New("boom")
	provider := &stubBidPriceProvider{err: wantErr}
	cache := NewPriceCache(provider)

	err := cache.UpdatePrices(armadacontext.Background())
	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, cache.CurrentSnapshot())
}
