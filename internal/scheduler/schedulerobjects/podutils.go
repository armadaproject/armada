package schedulerobjects

import (
	v1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"

	log "github.com/sirupsen/logrus"

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

func PodRequirementsFromPodSpec(podSpec *v1.PodSpec, priorityByPriorityClassName map[string]int32) *PodRequirements {
	priority, ok := PriorityFromPodSpec(podSpec, priorityByPriorityClassName)
	if !ok {
		log.Errorf("failed to get priority from priorityClassName %s", podSpec.PriorityClassName)
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
		Priority:             priority,
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

func ResourceListFromV1ResourceList(rl v1.ResourceList) ResourceList {
	rv := ResourceList{
		Resources: make(map[string]resource.Quantity),
	}
	for t, q := range rl {
		rv.Resources[string(t)] = q
	}
	return rv
}

// PriorityFromPodSpec returns the priority set in a pod spec.
// If priority is set diectly, that value is returned.
// Otherwise, it returns priorityByPriorityClassName[podSpec.PriorityClassName].
// ok is false if no priority is set for this pod spec, in which case priority is 0.
func PriorityFromPodSpec(podSpec *v1.PodSpec, priorityByPriorityClassName map[string]int32) (priority int32, ok bool) {
	if podSpec == nil {
		return
	}
	if podSpec.Priority != nil {
		priority = *podSpec.Priority
		ok = true
	} else if priorityByPriorityClassName != nil {
		priority, ok = priorityByPriorityClassName[podSpec.PriorityClassName]
	}
	return
}
