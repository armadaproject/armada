package bidstore

func (q *PoolBids) GetBidsForBand(band PriceBand) (*PriceBandBid, bool) {
	for _, bids := range q.PriceBandBids {
		if bids.PriceBand == band {
			return bids, true
		}
	}
	return nil, false
}

func (p *PriceBandBids) GetBidForPhase(phase PricingPhase) (*Bid, bool) {
	for _, phaseBid := range p.PricingPhaseBids {
		if phaseBid.PricingPhase == phase {
			return phaseBid.Bid, true
		}
	}
	return nil, false
}
