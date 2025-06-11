package metrics

import "github.com/armadaproject/armada/pkg/bidstore"

func GetPriceBandShortName(priceBand bidstore.PriceBand) string {
	result, valid := bidstore.PriceBandToShortName[priceBand]
	if !valid {
		return "Unknown"
	}
	return result
}
