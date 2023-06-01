package adapters

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// PodRequirementsFromPod function creates the schedulerobjects and creates a value for the
// annotation field by supplying it with a cloned value of pod.Annotations
func PodRequirementsFromPod(pod *v1.Pod, priorityByPriorityClassName map[string]configuration.PriorityClass) *schedulerobjects.PodRequirements {
	rv := PodRequirementsFromPodSpec(&pod.Spec, priorityByPriorityClassName)
	rv.Annotations = maps.Clone(pod.Annotations)
	return rv
}

// PodRequirementsFromPodSpec function returns *schedulerobjects.PodRequirements for podSpec.
// An error is logged if the podSpec uses an unknown priority class.
// This function may mutate podSpec.
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
	var resourceRequirements v1.ResourceRequirements
	if container := MergePodSpecContainers(podSpec); container != nil {
		resourceRequirements = container.Resources
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

// MergePodSpecContainers merges all containers and initContainers in podSpec into a single container
// and returns the resulting container. The requests and limits of this container is set to
//
// max(
//
//	sum across all containers,
//	max over all init containers,
//
// )
//
// This is because containers run in parallel, whereas initContainers run serially.
// All containers (and initContainers) other than the collapsed container are deleted from podSpec.
//
// This function mutates podSpec containers in-place to avoid allocating memory.
// This function is idempotent.
func MergePodSpecContainers(podSpec *v1.PodSpec) *v1.Container {
	containersMinIndex := 0
	initContainersMinIndex := 0
	var container *v1.Container
	if len(podSpec.Containers) > 0 {
		container = &podSpec.Containers[0]
		containersMinIndex = 1
	} else if len(podSpec.InitContainers) > 0 {
		container = &podSpec.InitContainers[0]
		initContainersMinIndex = 1
	}
	for i, c := range podSpec.Containers {
		if i < containersMinIndex {
			continue
		}
		for t, request := range c.Resources.Requests {
			q := container.Resources.Requests[t]
			q.Add(request)
			container.Resources.Requests[t] = q
		}
		for t, limit := range c.Resources.Limits {
			q := container.Resources.Limits[t]
			q.Add(limit)
			container.Resources.Limits[t] = q
		}
	}
	for i, c := range podSpec.InitContainers {
		if i < initContainersMinIndex {
			continue
		}
		for t, request := range c.Resources.Requests {
			if request.Cmp(container.Resources.Requests[t]) == 1 {
				container.Resources.Requests[t] = request
			}
		}
		for t, limit := range c.Resources.Limits {
			if limit.Cmp(container.Resources.Limits[t]) == 1 {
				container.Resources.Limits[t] = limit
			}
		}
	}
	if len(podSpec.Containers) > 0 {
		podSpec.Containers = podSpec.Containers[0:1]
	} else if len(podSpec.InitContainers) > 0 {
		podSpec.Containers = podSpec.InitContainers[0:1]
	}
	podSpec.InitContainers = nil
	return container
}

// PriorityFromPodSpec returns the priority in a pod spec.
// If priority is set directly, that value is returned.
// Otherwise, it returns the value of the key podSpec.
// In both cases the value along with true boolean is returned.
// PriorityClassName in priorityByPriorityClassName map.
// If no priority is set for the pod spec, 0 along with a false boolean would be returned
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
