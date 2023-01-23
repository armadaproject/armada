package domain

import armadaresource "github.com/armadaproject/armada/internal/common/resource"

const (
	AcceleratorDutyCycle = "armadaproject.io/accelerator-duty-cycle"
	AcceleratorMemory    = "armadaproject.io/accelerator-memory"
)

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

func (a *UtilisationData) Max(b *UtilisationData) {
	a.CurrentUsage.Max(b.CurrentUsage)
	a.CumulativeUsage.Max(b.CumulativeUsage)
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
