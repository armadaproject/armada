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

var PriceBandShortNames = map[string]PriceBand{
	"A": PriceBand_PRICE_BAND_A,
	"B": PriceBand_PRICE_BAND_B,
	"C": PriceBand_PRICE_BAND_C,
	"D": PriceBand_PRICE_BAND_D,
	"E": PriceBand_PRICE_BAND_E,
	"F": PriceBand_PRICE_BAND_F,
	"G": PriceBand_PRICE_BAND_G,
	"H": PriceBand_PRICE_BAND_H,
}
