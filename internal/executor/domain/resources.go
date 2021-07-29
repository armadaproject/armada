package domain

import "github.com/G-Research/armada/internal/common"

const AcceleratorDutyCycle = "armadaproject.io/accelerator-duty-cycle"
const AcceleratorMemory = "armadaproject.io/accelerator-memory"

type UtilisationData struct {
	CurrentUsage    common.ComputeResources
	CumulativeUsage common.ComputeResources
}

func EmptyUtilisationData() *UtilisationData {
	return &UtilisationData{
		CurrentUsage:    common.ComputeResources{},
		CumulativeUsage: common.ComputeResources{},
	}
}

func (a *UtilisationData) Max(b *UtilisationData) *UtilisationData {
	maxCurrentUsage := a.CurrentUsage
	maxCurrentUsage.Max(a.CurrentUsage)
	maxCumulativeUsage := a.CumulativeUsage
	maxCumulativeUsage.Max(b.CumulativeUsage)

	return &UtilisationData{
		CurrentUsage:    maxCurrentUsage,
		CumulativeUsage: maxCumulativeUsage,
	}
}

func (u *UtilisationData) DeepCopy() *UtilisationData {
	return &UtilisationData{
		CurrentUsage:    u.CurrentUsage.DeepCopy(),
		CumulativeUsage: u.CumulativeUsage.DeepCopy(),
	}
}
