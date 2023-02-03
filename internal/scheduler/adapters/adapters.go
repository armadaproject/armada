package adapters

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/logging"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func PodRequirementsFromPod(pod *v1.Pod, priorityByPriorityClassName map[string]configuration.PriorityClass) *schedulerobjects.PodRequirements {
	rv := PodRequirementsFromPodSpec(&pod.Spec, priorityByPriorityClassName)
	// We need to be able to update annotations in-place.
	rv.Annotations = pod.Annotations
	return rv
}

func PodRequirementsFromPodSpec(podSpec *v1.PodSpec, priorityByPriorityClassName map[string]configuration.PriorityClass) *schedulerobjects.PodRequirements {
	priority, ok := PriorityFromPodSpec(podSpec, priorityByPriorityClassName)
	if !ok {
		err := errors.Errorf("unknown priorityClassName %s", podSpec.PriorityClassName)
		logging.WithStacktrace(logrus.NewEntry(logrus.New()), err).Error("failed to get priority from priorityClassName")
	}
	preemptionPolicy := string(v1.PreemptLowerPriority)
	if podSpec.PreemptionPolicy != nil {
		preemptionPolicy = string(*podSpec.PreemptionPolicy)
	}
	resourceRequirements := v1.ResourceRequirements{
		Limits: v1ResourceListFromComputeResources(
			armadaresource.TotalPodResourceLimit(podSpec),
		),
		Requests: v1ResourceListFromComputeResources(
			armadaresource.TotalPodResourceRequest(podSpec),
		),
	}
	return &schedulerobjects.PodRequirements{
		NodeSelector:         podSpec.NodeSelector,
		Affinity:             podSpec.Affinity,
		Tolerations:          podSpec.Tolerations,
		Priority:             priority,
		PreemptionPolicy:     preemptionPolicy,
		ResourceRequirements: resourceRequirements,
	}
}

func v1ResourceListFromComputeResources(resources armadaresource.ComputeResources) v1.ResourceList {
	rv := make(v1.ResourceList)
	for t, q := range resources {
		rv[v1.ResourceName(t)] = q
	}
	return rv
}

// PriorityFromPodSpec returns the priority set in a pod spec.
// If priority is set directly, that value is returned.
// Otherwise, it returns priorityByPriorityClassName[podSpec.PriorityClassName].
// ok is false if no priority is set for this pod spec, in which case priority is 0.
func PriorityFromPodSpec(podSpec *v1.PodSpec, priorityByPriorityClassName map[string]configuration.PriorityClass) (int32, bool) {
	// If there's no podspec there's nothing we can do
	if podSpec == nil {
		return 0, false
	}

	// If a priority is directly specified, use that
	if podSpec.Priority != nil {
		return *podSpec.Priority, true
	}

	// If we find a priority class use that
	priorityClass, ok := priorityByPriorityClassName[podSpec.PriorityClassName]
	if ok {
		return priorityClass.Priority, true
	}

	// Couldn't find anything
	return 0, false
}
