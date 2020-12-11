package scheduling

import (
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common"
)

type nodeTypeAllocation struct {
	taints             []v1.Taint
	labels             map[string]string
	nodeSize           common.ComputeResources
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
