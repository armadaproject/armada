package domain

import (
	"k8s.io/apimachinery/pkg/api/resource"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
)

type UtilisationData struct {
	currentUsage    armadaresource.ComputeResources
	maxCurrentUsage armadaresource.ComputeResources
	sumCurrentUsage armadaresource.ComputeResources
	numDataPoints   map[string]int64
	cumulativeUsage armadaresource.ComputeResources
}

func NewUtilisationData(
	resources map[string]resource.Quantity,
) *UtilisationData {
	ret := EmptyUtilisationData()
	for k, v := range resources {
		ret.UpdateCurrentUsage(k, v)
	}
	return ret
}

func EmptyUtilisationData() *UtilisationData {
	return &UtilisationData{
		currentUsage:    armadaresource.ComputeResources{},
		maxCurrentUsage: armadaresource.ComputeResources{},
		sumCurrentUsage: armadaresource.ComputeResources{},
		numDataPoints:   map[string]int64{},
		cumulativeUsage: armadaresource.ComputeResources{},
	}
}

func (a *UtilisationData) UpdateCurrentUsage(resource string, quantity resource.Quantity) {
	a.currentUsage[resource] = quantity
	a.maxCurrentUsage[resource] = quantity
	a.sumCurrentUsage[resource] = quantity
	a.numDataPoints[resource] = 1
}

func (a *UtilisationData) Process(b *UtilisationData) {
	a.currentUsage = b.currentUsage.DeepCopy()
	a.maxCurrentUsage.Max(b.maxCurrentUsage)
	a.sumCurrentUsage.Add(b.sumCurrentUsage)
	a.cumulativeUsage.Max(b.cumulativeUsage)

	for k, v := range b.numDataPoints {
		existing, ok := a.numDataPoints[k]
		if ok {
			a.numDataPoints[k] = existing + v
		} else {
			a.numDataPoints[k] = v
		}
	}
}

func (u *UtilisationData) GetCurrentUsage() armadaresource.ComputeResources {
	return u.currentUsage
}

func (u *UtilisationData) GetMaxUsage() armadaresource.ComputeResources {
	return u.maxCurrentUsage
}

func (u *UtilisationData) GetAvgUsage() armadaresource.ComputeResources {
	avg := u.sumCurrentUsage.DeepCopy()
	avg.Div(u.numDataPoints)
	return avg
}

func (u *UtilisationData) GetCumulativeUsage() armadaresource.ComputeResources {
	return u.cumulativeUsage
}

func (u *UtilisationData) DeepCopy() *UtilisationData {
	numDataPoints := map[string]int64{}

	for k, v := range u.numDataPoints {
		numDataPoints[k] = v
	}

	return &UtilisationData{
		currentUsage:    u.currentUsage.DeepCopy(),
		maxCurrentUsage: u.maxCurrentUsage.DeepCopy(),
		sumCurrentUsage: u.sumCurrentUsage.DeepCopy(),
		numDataPoints:   numDataPoints,
		cumulativeUsage: u.cumulativeUsage.DeepCopy(),
	}
}

func (u *UtilisationData) IsEmpty() bool {
	return len(u.cumulativeUsage) == 0 && len(u.currentUsage) == 0
}
