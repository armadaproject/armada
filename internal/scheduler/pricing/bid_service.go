package pricing

import (
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/bidstore"
)

//type BidPriceProvider interface {
//	GetQueueBidPrices(queues []string) (map[string]map[bidstore.PriceBand]*PriceBandInfo, error)
//	GetPricedPools() []string
//}
//
//type PriceBandInfo struct {
//	QueuedBidsPerPool  map[string]float64
//	RunningBidsPerPool map[string]float64
//}
//

//
//type StubBidPriceProvider struct {
//	Pools []string
//}
//
//func (s StubBidPriceProvider) GetQueueBidPrices(queues []string) (map[string]map[bidstore.PriceBand]*PriceBandInfo, error) {
//	result := map[string]map[bidstore.PriceBand]*PriceBandInfo{}
//
//	for _, queue := range queues {
//		queueResult := map[bidstore.PriceBand]*PriceBandInfo{}
//
//		for _, pool := range s.Pools {
//			for priceBand := range bidstore.PriceBand_name {
//				if _, exists := queueResult[bidstore.PriceBand(priceBand)]; !exists {
//					queueResult[bidstore.PriceBand(priceBand)] = &PriceBandInfo{
//						QueuedBidsPerPool:  map[string]float64{},
//						RunningBidsPerPool: map[string]float64{},
//					}
//				}
//
//				queueResult[bidstore.PriceBand(priceBand)].QueuedBidsPerPool[pool] = float64(priceBand)
//				queueResult[bidstore.PriceBand(priceBand)].RunningBidsPerPool[pool] = float64(priceBand)
//			}
//		}
//
//		result[queue] = queueResult
//	}
//
//	return result, nil
//}
//
//func (n StubBidPriceProvider) GetPricedPools() []string {
//	return n.Pools
//}
//
//type ExternalBidPriceInfo struct {
//	client bidstore.BidRetrieverServiceClient
//	pools  []string
//}
//
//func NewBidPriceService(client bidstore.BidRetrieverServiceClient, pools []string) *ExternalBidPriceInfo {
//	return &ExternalBidPriceInfo{
//		client: client,
//		pools:  pools,
//	}
//}
//
//func (b *ExternalBidPriceInfo) GetQueueBidPrices(queues []string) (map[string]map[bidstore.PriceBand]*PriceBandInfo, error) {
//	response, err := b.client.RetrieveBids(armadacontext.Background(), &bidstore.RetrieveBidsRequest{Queues: queues})
//	if err != nil {
//		return nil, err
//	}
//
//	result := map[string]map[bidstore.PriceBand]*PriceBandInfo{}
//
//	// TODO Make the returned value into a struct
//	// - Can be simpler to consume
//	// - Lots of this logic can be abstracted away
//	// - Easier to implement a diff func
//	for _, queue := range queues {
//		bids, present := response.QueueBids[queue]
//		if !present {
//			return nil, fmt.Errorf("no bid price information found for queue %s", queue)
//		}
//		queueResult := map[bidstore.PriceBand]*PriceBandInfo{}
//
//		for _, pool := range b.pools {
//			poolBids, present := bids.GetPoolBids()[pool]
//			if !present {
//				return nil, fmt.Errorf("no bid price information found for pool %s in queue %s", pool, queue)
//			}
//
//			defaultBids := poolBids.GetFallbackBid()
//			if defaultBids == nil {
//				return nil, fmt.Errorf("no fallback bid price information found for pool %s in queue %s", pool, queue)
//			}
//
//			defaultQueuedBid, present := defaultBids.GetBidForPhase(bidstore.PricingPhase_PRICING_PHASE_QUEUEING)
//			if !present {
//				return nil, fmt.Errorf("no fallback queued bid price information found for pool %s in queue %s", pool, queue)
//			}
//
//			defaultRunningBid, present := defaultBids.GetBidForPhase(bidstore.PricingPhase_PRICING_PHASE_RUNNING)
//			if !present {
//				return nil, fmt.Errorf("no fallback running bid price information found for pool %s in queue %s", pool, queue)
//			}
//
//			for priceBand := range bidstore.PriceBand_name {
//				queuedBidPrice := defaultQueuedBid.GetAmount()
//				runningBidPrice := defaultRunningBid.GetAmount()
//				bandBids, present := poolBids.GetBidsForBand(bidstore.PriceBand(priceBand))
//				if present {
//					queuedPrice, ok := bandBids.PriceBandBids.GetBidForPhase(bidstore.PricingPhase_PRICING_PHASE_QUEUEING)
//					if ok {
//						queuedBidPrice = queuedPrice.Amount
//					}
//					runningPrice, ok := bandBids.PriceBandBids.GetBidForPhase(bidstore.PricingPhase_PRICING_PHASE_RUNNING)
//					if ok {
//						queuedBidPrice = runningPrice.Amount
//					}
//				}
//
//				if _, exists := queueResult[bidstore.PriceBand(priceBand)]; !exists {
//					queueResult[bidstore.PriceBand(priceBand)] = &PriceBandInfo{
//						QueuedBidsPerPool:  map[string]float64{},
//						RunningBidsPerPool: map[string]float64{},
//					}
//				}
//				queueResult[bidstore.PriceBand(priceBand)].QueuedBidsPerPool[pool] = queuedBidPrice
//				queueResult[bidstore.PriceBand(priceBand)].RunningBidsPerPool[pool] = runningBidPrice
//			}
//		}
//
//		result[queue] = queueResult
//	}
//
//	return result, nil
//}
//
//func (b *ExternalBidPriceInfo) GetPricedPools() []string {
//	return slices.Clone(b.pools)
//}

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
