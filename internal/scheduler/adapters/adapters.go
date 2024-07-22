package adapters

import (
	v1 "k8s.io/api/core/v1"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

// PodRequirementsFromPodSpec function returns *schedulerobjects.PodRequirements for podSpec.
// An error is logged if the podSpec uses an unknown priority class.
// This function may mutate podSpec.
func PodRequirementsFromPodSpec(podSpec *v1.PodSpec) *schedulerobjects.PodRequirements {
	preemptionPolicy := string(v1.PreemptLowerPriority)
	if podSpec.PreemptionPolicy != nil {
		preemptionPolicy = string(*podSpec.PreemptionPolicy)
	}
	return &schedulerobjects.PodRequirements{
		NodeSelector:         podSpec.NodeSelector,
		Affinity:             podSpec.Affinity,
		Tolerations:          podSpec.Tolerations,
		PreemptionPolicy:     preemptionPolicy,
		ResourceRequirements: api.SchedulingResourceRequirementsFromPodSpec(podSpec),
	}
}

func K8sResourceListToMap(resources v1.ResourceList) map[string]k8sResource.Quantity {
	if resources == nil {
		return nil
	}
	result := make(map[string]k8sResource.Quantity, len(resources))
	for k, v := range resources {
		result[string(k)] = v
	}
	return result
}
