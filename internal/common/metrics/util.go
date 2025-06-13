package metrics

import "github.com/armadaproject/armada/pkg/bidstore"

const UnknownPriceBandName = "Unknown"

func GetPriceBandShortName(priceBand bidstore.PriceBand) string {
	result, valid := bidstore.PriceBandToShortName[priceBand]
	if !valid {
		return UnknownPriceBandName
	}
	return result
}
