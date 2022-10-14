package schedulerobjects

import (
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common"
)

func (req *PodRequirements) GetAffinityNodeSelector() *v1.NodeSelector {
	affinity := req.Affinity
	if affinity == nil {
		return nil
	}
	nodeAffinity := affinity.NodeAffinity
	if nodeAffinity == nil {
		return nil
	}
	return nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
}

func PodRequirementsFromPodSpec(podSpec *v1.PodSpec) *PodRequirements {
	var priority int32
	if podSpec.Priority != nil {
		priority = *podSpec.Priority
	}
	preemptionPolicy := string(v1.PreemptLowerPriority)
	if podSpec.PreemptionPolicy != nil {
		preemptionPolicy = string(*podSpec.PreemptionPolicy)
	}
	resourceRequirements := v1.ResourceRequirements{
		Limits: v1ResourceListFromComputeResources(
			common.TotalPodResourceLimit(podSpec),
		),
		Requests: v1ResourceListFromComputeResources(
			common.TotalPodResourceRequest(podSpec),
		),
	}
	return &PodRequirements{
		NodeSelector:         podSpec.NodeSelector,
		Affinity:             podSpec.Affinity,
		Tolerations:          podSpec.Tolerations,
		Priority:             priority, // TODO: May not be set here.
		PreemptionPolicy:     preemptionPolicy,
		ResourceRequirements: resourceRequirements,
	}
}

func v1ResourceListFromComputeResources(resources common.ComputeResources) v1.ResourceList {
	rv := make(v1.ResourceList)
	for t, q := range resources {
		rv[v1.ResourceName(t)] = q
	}
	return rv
}
