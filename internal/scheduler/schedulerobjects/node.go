package schedulerobjects

import "github.com/armadaproject/armada/internal/common/resource"

func (node *Node) AvailableArmadaResource() resource.ComputeResources {
	cr := node.TotalResources.ToComputeResources()
	for _, rl := range node.UnallocatableResources {
		cr.Sub(rl.ToComputeResources())
	}
	return cr
}
