package adapters

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/logging"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// PodRequirementsFromPod function creates the schedulerobjects and creates a value for the
// Annotation field by supplying it with a cloned value of pod.Annotations
func PodRequirementsFromPod(pod *v1.Pod, priorityByPriorityClassName map[string]configuration.PriorityClass) *schedulerobjects.PodRequirements {
	rv := PodRequirementsFromPodSpec(&pod.Spec, priorityByPriorityClassName)
	rv.Annotations = maps.Clone(pod.Annotations)
	return rv
}

// PodRequirementsFromPodSpec function returns the *schedulerobjects.PodRequirements  object.
// It logs an error if priority is set using priorityClassName and there is no
// corresponding entry in the priorityByPriorityClassName map.
func PodRequirementsFromPodSpec(podSpec *v1.PodSpec, priorityByPriorityClassName map[string]configuration.PriorityClass) *schedulerobjects.PodRequirements {
	priority, ok := PriorityFromPodSpec(podSpec, priorityByPriorityClassName)
	if priorityByPriorityClassName != nil && !ok {
		// Ignore this error if priorityByPriorityClassName is explicitly set to nil.
		// We assume that in this case the caller is sure the priority does not need to be set.
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

// v1ResourceListFromComputeResources function converts the armadaresource.ComputeResources type
// defined as map[string]resource.Quantity to v1.ResourceList defined in the k8s API as
// map[ResourceName]resource.Quantity
func v1ResourceListFromComputeResources(resources armadaresource.ComputeResources) v1.ResourceList {
	rv := make(v1.ResourceList)
	for t, q := range resources {
		rv[v1.ResourceName(t)] = q
	}
	return rv
}

// PriorityFromPodSpec returns the priority in a pod spec.
// If priority is set directly, that value is returned.
// Otherwise, it returns the value of the key podSpec.PriorityClassName in priorityByPriorityClassName map.
// If no priority is set for the pod spec, 0 along with a false value would be returned
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
