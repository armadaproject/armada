package pricing

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/bidstore"
)

type BidPriceProvider interface {
	GetQueueBidPrices(queues []string) (map[string]map[bidstore.PriceBand]map[string]float64, error)
}

type StubBidPriceProvider struct {
	pools []string
}

func (s StubBidPriceProvider) GetQueueBidPrices(queues []string) (map[string]map[bidstore.PriceBand]map[string]float64, error) {
	result := map[string]map[bidstore.PriceBand]map[string]float64{}

	for _, queue := range queues {
		queueResult := map[bidstore.PriceBand]map[string]float64{}

		for _, pool := range s.pools {
			for priceBand := range bidstore.PriceBand_name {
				if _, exists := queueResult[bidstore.PriceBand(priceBand)]; !exists {
					queueResult[bidstore.PriceBand(priceBand)] = map[string]float64{}
				}
				queueResult[bidstore.PriceBand(priceBand)][pool] = 1
			}
		}

		result[queue] = queueResult
	}

	return result, nil
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

func (b *ExternalBidPriceInfo) GetQueueBidPrices(queues []string) (map[string]map[bidstore.PriceBand]map[string]float64, error) {
	response, err := b.client.RetrieveBids(armadacontext.Background(), &bidstore.RetrieveBidsRequest{Queues: queues})
	if err != nil {
		return nil, err
	}

	result := map[string]map[bidstore.PriceBand]map[string]float64{}

	for _, queue := range queues {
		bids, present := response.QueueBids[queue]
		if !present {
			return nil, fmt.Errorf("no bid price information found for queue %s", queue)
		}
		queueResult := map[bidstore.PriceBand]map[string]float64{}

		for _, pool := range b.pools {
			poolBids, present := bids.GetPoolBids()[pool]
			if !present {
				return nil, fmt.Errorf("no bid price information found for pool %s in queue %s", pool, queue)
			}

			defaultBid, present := poolBids.GetBidsForBand(bidstore.PriceBand_PRICE_BAND_QUEUE_FALLBACK)
			if !present {
				return nil, fmt.Errorf("no fallback bid price information found for pool %s in queue %s", pool, queue)
			}

			defaultRunningBid, present := defaultBid.GetPriceBandBids().GetBidForPhase(bidstore.PricingPhase_PRICING_PHASE_RUNNING)
			if !present {
				return nil, fmt.Errorf("no fallback running bid price information found for pool %s in queue %s", pool, queue)
			}

			for priceBand := range bidstore.PriceBand_name {
				bidPrice := defaultRunningBid.GetAmount()
				bandBids, present := poolBids.GetBidsForBand(bidstore.PriceBand(priceBand))
				if present {
					runningBid, ok := bandBids.PriceBandBids.GetBidForPhase(bidstore.PricingPhase_PRICING_PHASE_RUNNING)
					if ok {
						bidPrice = runningBid.Amount
					}
				}

				if _, exists := queueResult[bidstore.PriceBand(priceBand)]; !exists {
					queueResult[bidstore.PriceBand(priceBand)] = map[string]float64{}
				}
				queueResult[bidstore.PriceBand(priceBand)][pool] = bidPrice
			}
		}

		result[queue] = queueResult
	}

	return result, nil
}
