package scheduling

import (
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
)

type nodeTypeAllocation struct {
	nodeType           api.NodeType
	availableResources common.ComputeResourcesFloat
}

type nodeTypeUsedResources map[*nodeTypeAllocation]common.ComputeResourcesFloat

func (r nodeTypeUsedResources) DeepCopy() map[*nodeTypeAllocation]common.ComputeResourcesFloat {
	result := map[*nodeTypeAllocation]common.ComputeResourcesFloat{}
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
