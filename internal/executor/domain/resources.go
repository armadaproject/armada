package domain

import armadaresource "github.com/armadaproject/armada/internal/common/resource"

type UtilisationData struct {
	CurrentUsage    armadaresource.ComputeResources
	CumulativeUsage armadaresource.ComputeResources
}

func EmptyUtilisationData() *UtilisationData {
	return &UtilisationData{
		CurrentUsage:    armadaresource.ComputeResources{},
		CumulativeUsage: armadaresource.ComputeResources{},
	}
}

func (u *UtilisationData) DeepCopy() *UtilisationData {
	return &UtilisationData{
		CurrentUsage:    u.CurrentUsage.DeepCopy(),
		CumulativeUsage: u.CumulativeUsage.DeepCopy(),
	}
}

func (u *UtilisationData) IsEmpty() bool {
	return len(u.CumulativeUsage) == 0 && len(u.CurrentUsage) == 0
}

type UtilisationDataAggregation struct {
	maxUsage        armadaresource.ComputeResources
	sumOfUsage      armadaresource.ComputeResources
	numDataPoints   map[string]int64
	cumulativeUsage armadaresource.ComputeResources
}

func EmptyUtilisationDataAggregation() *UtilisationDataAggregation {
	return &UtilisationDataAggregation{
		maxUsage:        armadaresource.ComputeResources{},
		sumOfUsage:      armadaresource.ComputeResources{},
		numDataPoints:   map[string]int64{},
		cumulativeUsage: armadaresource.ComputeResources{},
	}
}

func NewUtilisationDataAggregation(u *UtilisationData) *UtilisationDataAggregation {
	numDataPoints := make(map[string]int64)

	for k := range u.CurrentUsage {
		numDataPoints[k] = 1
	}

	return &UtilisationDataAggregation{
		maxUsage:        u.CurrentUsage.DeepCopy(),
		sumOfUsage:      u.CurrentUsage.DeepCopy(),
		numDataPoints:   numDataPoints,
		cumulativeUsage: u.CumulativeUsage.DeepCopy(),
	}
}

func (u *UtilisationDataAggregation) GetMaxUsage() armadaresource.ComputeResources {
	return u.maxUsage
}

func (u *UtilisationDataAggregation) GetAvgUsage() armadaresource.ComputeResources {
	avg := u.sumOfUsage.DeepCopy()
	avg.Div(u.numDataPoints)
	return avg
}

func (u *UtilisationDataAggregation) GetCumulativeUsage() armadaresource.ComputeResources {
	return u.cumulativeUsage
}

func (a *UtilisationDataAggregation) Add(b *UtilisationData) {
	a.maxUsage.Max(b.CurrentUsage)
	a.sumOfUsage.Add(b.CurrentUsage)
	a.cumulativeUsage.Max(b.CumulativeUsage)

	for k := range b.CurrentUsage {
		existing, ok := a.numDataPoints[k]
		if ok {
			a.numDataPoints[k] = existing + 1
		} else {
			a.numDataPoints[k] = 1
		}
	}
}

func (u *UtilisationDataAggregation) DeepCopy() *UtilisationDataAggregation {
	numDataPoints := map[string]int64{}

	for k, v := range u.numDataPoints {
		numDataPoints[k] = v
	}

	return &UtilisationDataAggregation{
		maxUsage:        u.maxUsage.DeepCopy(),
		sumOfUsage:      u.sumOfUsage.DeepCopy(),
		numDataPoints:   numDataPoints,
		cumulativeUsage: u.cumulativeUsage.DeepCopy(),
	}
}

func (u *UtilisationDataAggregation) IsEmpty() bool {
	return len(u.maxUsage) == 0
}
