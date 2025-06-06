package pricing

import (
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/pkg/market"
)

type BidPriceProvider interface {
	GetJobSetsBidPrices(jobSets []*market.QueueJobSet) (map[*market.QueueJobSet]map[string]float64, error)
}

type StubBidPriceProvider struct{}

func (s StubBidPriceProvider) GetJobSetsBidPrices(jobSets []*market.QueueJobSet) (map[*market.QueueJobSet]map[string]float64, error) {
	result := map[*market.QueueJobSet]map[string]float64{}
	pools := []string{"default"}
	for _, jobSet := range jobSets {
		jobSetResult := map[string]float64{}
		for _, pool := range pools {
			jobSetResult[pool] = 1
		}
		result[jobSet] = jobSetResult
	}

	return result, nil
}

type ExternalBidPriceInfo struct {
	client market.BidPriceServiceClient
}

func NewBidPriceService(client market.BidPriceServiceClient) *ExternalBidPriceInfo {
	return &ExternalBidPriceInfo{
		client: client,
	}
}

func (b *ExternalBidPriceInfo) GetJobSetsBidPrices(jobSets []*market.QueueJobSet) (map[*market.QueueJobSet]map[string]float64, error) {
	response, err := b.client.GetJobSetBids(armadacontext.Background(), &market.BidQueryRequest{QueueJobSet: jobSets})
	if err != nil {
		return nil, err
	}

	pools := maps.Keys(response.BidsByPool)
	result := map[*market.QueueJobSet]map[string]float64{}

	for _, jobSet := range jobSets {
		jobSetResult := map[string]float64{}
		for _, pool := range pools {
			queueBids, present := response.GetBidsByPool()[pool].GetBidsByQueue()[jobSet.Queue]
			if !present {
				continue
			}
			jobSetBid, present := queueBids.BidsByJobSet[jobSet.JobSet]
			if !present {
				continue
			}

			jobSetResult[pool] = float64(jobSetBid.BidPrice)
		}
		result[jobSet] = jobSetResult
	}

	return result, nil
}
