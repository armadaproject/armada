package schedulerobjects

import (
	"github.com/armadaproject/armada/internal/common/constants"
	"github.com/armadaproject/armada/internal/common/resource"
)

func (node *Node) AvailableArmadaResource() resource.ComputeResources {
	cr := node.TotalResources.ToComputeResources()
	for _, rl := range node.UnallocatableResources {
		cr.Sub(rl.ToComputeResources())
	}
	cr.LimitToZero()
	return cr
}

func (node *Node) ReservationName() string {
	// TODO Make this part of the struct so its cheaper than calculating it each time
	for _, taint := range node.Taints {
		if taint.Key == constants.ReservationTaintKey {
			return taint.Value
		}
	}
	return ""
}
