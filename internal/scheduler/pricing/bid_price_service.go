package pricing

import (
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/bidstore"
)

var (
	allBands  = initAllBands()
	allPhases = initAllPhases()
)

type ExternalBidPriceService struct {
	client bidstore.BidRetrieverServiceClient
}

func NewExternalBidPriceService(client bidstore.BidRetrieverServiceClient) *ExternalBidPriceService {
	return &ExternalBidPriceService{
		client: client,
	}
}

func (b *ExternalBidPriceService) GetBidPrices(
	ctx *armadacontext.Context,
) (BidPriceSnapshot, error) {
	resp, err := b.client.RetrieveBids(ctx, &bidstore.RetrieveBidsRequest{})
	if err != nil {
		return BidPriceSnapshot{}, err
	}

	snapshot := BidPriceSnapshot{
		Timestamp: time.Now(),
		Prices:    make(map[PriceKey]float64),
	}

	for queue, qb := range resp.QueueBids {
		for pool, poolBids := range qb.PoolBids {
			// get fallback if present, but don’t error if nil
			fallback := poolBids.GetFallbackBid()

			for _, band := range allBands {
				for _, phase := range allPhases {
					var price float64
					var hasBid bool

					// 1) check for band-specific override
					if bb, found := poolBids.GetBidsForBand(band); found {
						if ov, ok := bb.PriceBandBids.GetBidForPhase(phase); ok {
							price = ov.Amount
							hasBid = true
						}
					}

					// 2) if no override and fallback exists, use fallback
					if !hasBid && fallback != nil {
						if def, ok := fallback.GetBidForPhase(phase); ok {
							price = def.Amount
							hasBid = true
						}
					}

					// 3) if neither override nor fallback had this phase, skip
					if !hasBid {
						continue
					}

					// 4) record it
					key := PriceKey{
						Queue: queue,
						Pool:  pool,
						Band:  band,
						Phase: phase,
					}
					snapshot.Prices[key] = price
				}
			}
		}
	}

	return snapshot, nil
}

func initAllBands() []bidstore.PriceBand {
	bands := make([]bidstore.PriceBand, 0, len(bidstore.PriceBand_name))
	for v := range bidstore.PriceBand_name {
		bands = append(bands, bidstore.PriceBand(v))
	}
	return bands
}

func initAllPhases() []bidstore.PricingPhase {
	phases := make([]bidstore.PricingPhase, 0, len(bidstore.PricingPhase_name))
	for v := range bidstore.PricingPhase_name {
		phases = append(phases, bidstore.PricingPhase(v))
	}
	return phases
}
