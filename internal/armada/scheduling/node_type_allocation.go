package scheduling

import (
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/pkg/api"
)

// nodeTypeAllocation stores the available resources for all nodes of a specific node type.
type nodeTypeAllocation struct {
	nodeType           api.NodeType
	availableResources armadaresource.ComputeResourcesFloat
	totalResources     armadaresource.ComputeResourcesFloat
	allocatedResources map[int32]armadaresource.ComputeResourcesFloat
}

type nodeTypeUsedResources map[*nodeTypeAllocation]armadaresource.ComputeResourcesFloat

func (r nodeTypeUsedResources) DeepCopy() map[*nodeTypeAllocation]armadaresource.ComputeResourcesFloat {
	result := map[*nodeTypeAllocation]armadaresource.ComputeResourcesFloat{}
	for k, v := range r {
		result[k] = v.DeepCopy()
	}
	return result
}

func (r nodeTypeUsedResources) Add(consumed nodeTypeUsedResources) {
	for nodeType, resources := range consumed {
		newResources := resources.DeepCopy()
		newResources.Add(r[nodeType])
		r[nodeType] = newResources
	}
}
