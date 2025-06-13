package pricing

import (
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/bidstore"
)

var allBands = initAllBands()

type NoopBidPriceProvider struct{}

func (n NoopBidPriceProvider) GetBidPrices(ctx *armadacontext.Context) (BidPriceSnapshot, error) {
	return BidPriceSnapshot{}, nil
}

type ExternalBidPriceService struct {
	client bidstore.BidRetrieverServiceClient
}

func NewExternalBidPriceService(client bidstore.BidRetrieverServiceClient) *ExternalBidPriceService {
	return &ExternalBidPriceService{
		client: client,
	}
}

func (b *ExternalBidPriceService) GetBidPrices(ctx *armadacontext.Context) (BidPriceSnapshot, error) {
	resp, err := b.client.RetrieveBids(ctx, &bidstore.RetrieveBidsRequest{})
	if err != nil {
		return BidPriceSnapshot{}, err
	}
	return convert(resp), nil
}

func convert(resp *bidstore.RetrieveBidsResponse) BidPriceSnapshot {
	snapshot := BidPriceSnapshot{
		Timestamp: time.Now(),
		Bids:      make(map[PriceKey]map[string]Bid),
	}

	for queue, qb := range resp.QueueBids {
		for _, band := range allBands {
			key := PriceKey{Queue: queue, Band: band}
			bids := make(map[string]Bid)

			for pool, poolBids := range qb.PoolBids {
				bb, _ := poolBids.GetBidsForBand(band)
				fallback := poolBids.GetFallbackBid()

				queued, hasQueued := getPrice(bb, fallback, bidstore.PricingPhase_PRICING_PHASE_QUEUEING)
				running, hasRunning := getPrice(bb, fallback, bidstore.PricingPhase_PRICING_PHASE_RUNNING)

				if hasQueued || hasRunning {
					bids[pool] = Bid{
						QueuedBid:  queued,
						RunningBid: running,
					}
				}
			}

			if len(bids) > 0 {
				snapshot.Bids[key] = bids
			}
		}
	}
	return snapshot
}

// getPrice tries to find a price for the given phase from bb or fallback.
func getPrice(
	bb *bidstore.PriceBandBid,
	fallback *bidstore.PriceBandBids,
	phase bidstore.PricingPhase,
) (float64, bool) {
	if bb != nil {
		if bid, ok := bb.PriceBandBids.GetBidForPhase(phase); ok {
			return bid.Amount, true
		}
	}
	if fallback != nil {
		if bid, ok := fallback.GetBidForPhase(phase); ok {
			return bid.Amount, true
		}
	}
	return 0, false
}

func initAllBands() []bidstore.PriceBand {
	bands := make([]bidstore.PriceBand, 0, len(bidstore.PriceBand_name))
	for v := range bidstore.PriceBand_name {
		band := bidstore.PriceBand(v)
		if band == bidstore.PriceBand_PRICE_BAND_UNSPECIFIED {
			continue
		}
		bands = append(bands, band)
	}
	return bands
}
