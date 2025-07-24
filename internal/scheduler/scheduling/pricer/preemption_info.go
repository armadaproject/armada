package pricer

import (
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

type jobDetails struct {
	queue     string
	jobId     string
	resources internaltypes.ResourceList
	cost      float64
	ageMillis int64
}

type priceOrder []*jobDetails

func (gpo priceOrder) Len() int {
	return len(gpo)
}

func (gpo priceOrder) Less(i, j int) bool {
	if gpo[i].cost != gpo[j].cost {
		return gpo[i].cost < gpo[j].cost
	}
	if gpo[i].ageMillis != gpo[j].ageMillis {
		return gpo[i].ageMillis < gpo[j].ageMillis
	}
	return gpo[i].jobId < gpo[j].jobId
}

func (gpo priceOrder) Swap(i, j int) {
	gpo[i], gpo[j] = gpo[j], gpo[i]
}
