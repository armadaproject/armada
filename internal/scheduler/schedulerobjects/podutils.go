package schedulerobjects

import v1 "k8s.io/api/core/v1"

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
		Limits:   make(v1.ResourceList),
		Requests: make(v1.ResourceList),
	}
	for _, container := range podSpec.Containers {
		for resourceType, quantity := range container.Resources.Limits {
			q := resourceRequirements.Limits[resourceType]
			q.Add(quantity)
			resourceRequirements.Limits[resourceType] = q

		}
		for resourceType, quantity := range container.Resources.Requests {
			q := resourceRequirements.Requests[resourceType]
			q.Add(quantity)
			resourceRequirements.Requests[resourceType] = q
		}
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
