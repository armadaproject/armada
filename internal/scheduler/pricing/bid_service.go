package pricing

import (
	"fmt"

	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/bidstore"
)

type BidPriceProvider interface {
	GetQueueBidPrices(queues []string) (map[string]map[bidstore.PriceBand]*PriceBandInfo, error)
	GetPricedPools() []string
}

type PriceBandInfo struct {
	QueuedBidsPerPool  map[string]float64
	RunningBidsPerPool map[string]float64
}

type NoopBidPriceProvider struct{}

func (n NoopBidPriceProvider) GetQueueBidPrices(queues []string) (map[string]map[bidstore.PriceBand]*PriceBandInfo, error) {
	return map[string]map[bidstore.PriceBand]*PriceBandInfo{}, nil
}

func (n NoopBidPriceProvider) GetPricedPools() []string {
	return []string{}
}

type StubBidPriceProvider struct {
	Pools []string
}

func (s StubBidPriceProvider) GetQueueBidPrices(queues []string) (map[string]map[bidstore.PriceBand]*PriceBandInfo, error) {
	result := map[string]map[bidstore.PriceBand]*PriceBandInfo{}

	for _, queue := range queues {
		queueResult := map[bidstore.PriceBand]*PriceBandInfo{}

		for _, pool := range s.Pools {
			for priceBand := range bidstore.PriceBand_name {
				if _, exists := queueResult[bidstore.PriceBand(priceBand)]; !exists {
					queueResult[bidstore.PriceBand(priceBand)] = &PriceBandInfo{
						QueuedBidsPerPool:  map[string]float64{},
						RunningBidsPerPool: map[string]float64{},
					}
				}

				queueResult[bidstore.PriceBand(priceBand)].QueuedBidsPerPool[pool] = float64(priceBand)
				queueResult[bidstore.PriceBand(priceBand)].RunningBidsPerPool[pool] = float64(priceBand)
			}
		}

		result[queue] = queueResult
	}

	return result, nil
}

func (n StubBidPriceProvider) GetPricedPools() []string {
	return n.Pools
}

type ExternalBidPriceInfo struct {
	client bidstore.BidRetrieverServiceClient
	pools  []string
}

func NewBidPriceService(client bidstore.BidRetrieverServiceClient, pools []string) *ExternalBidPriceInfo {
	return &ExternalBidPriceInfo{
		client: client,
		pools:  pools,
	}
}

func (b *ExternalBidPriceInfo) GetQueueBidPrices(queues []string) (map[string]map[bidstore.PriceBand]*PriceBandInfo, error) {
	response, err := b.client.RetrieveBids(armadacontext.Background(), &bidstore.RetrieveBidsRequest{Queues: queues})
	if err != nil {
		return nil, err
	}

	result := map[string]map[bidstore.PriceBand]*PriceBandInfo{}

	// TODO Make the returned value into a struct
	// - Can be simpler to consume
	// - Lots of this logic can be abstracted away
	// - Easier to implement a diff func
	for _, queue := range queues {
		bids, present := response.QueueBids[queue]
		if !present {
			return nil, fmt.Errorf("no bid price information found for queue %s", queue)
		}
		queueResult := map[bidstore.PriceBand]*PriceBandInfo{}

		for _, pool := range b.pools {
			poolBids, present := bids.GetPoolBids()[pool]
			if !present {
				return nil, fmt.Errorf("no bid price information found for pool %s in queue %s", pool, queue)
			}

			defaultBids := poolBids.GetFallbackBid()
			if defaultBids == nil {
				return nil, fmt.Errorf("no fallback bid price information found for pool %s in queue %s", pool, queue)
			}

			defaultQueuedBid, present := defaultBids.GetBidForPhase(bidstore.PricingPhase_PRICING_PHASE_QUEUEING)
			if !present {
				return nil, fmt.Errorf("no fallback queued bid price information found for pool %s in queue %s", pool, queue)
			}

			defaultRunningBid, present := defaultBids.GetBidForPhase(bidstore.PricingPhase_PRICING_PHASE_RUNNING)
			if !present {
				return nil, fmt.Errorf("no fallback running bid price information found for pool %s in queue %s", pool, queue)
			}

			for priceBand := range bidstore.PriceBand_name {
				queuedBidPrice := defaultQueuedBid.GetAmount()
				runningBidPrice := defaultRunningBid.GetAmount()
				bandBids, present := poolBids.GetBidsForBand(bidstore.PriceBand(priceBand))
				if present {
					queuedPrice, ok := bandBids.PriceBandBids.GetBidForPhase(bidstore.PricingPhase_PRICING_PHASE_QUEUEING)
					if ok {
						queuedBidPrice = queuedPrice.Amount
					}
					runningPrice, ok := bandBids.PriceBandBids.GetBidForPhase(bidstore.PricingPhase_PRICING_PHASE_RUNNING)
					if ok {
						queuedBidPrice = runningPrice.Amount
					}
				}

				if _, exists := queueResult[bidstore.PriceBand(priceBand)]; !exists {
					queueResult[bidstore.PriceBand(priceBand)] = &PriceBandInfo{
						QueuedBidsPerPool:  map[string]float64{},
						RunningBidsPerPool: map[string]float64{},
					}
				}
				queueResult[bidstore.PriceBand(priceBand)].QueuedBidsPerPool[pool] = queuedBidPrice
				queueResult[bidstore.PriceBand(priceBand)].RunningBidsPerPool[pool] = runningBidPrice
			}
		}

		result[queue] = queueResult
	}

	return result, nil
}

func (b *ExternalBidPriceInfo) GetPricedPools() []string {
	return slices.Clone(b.pools)
}
