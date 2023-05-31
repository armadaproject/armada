package adapters

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/logging"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
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
	CollapsePodSpecContainers(podSpec) // May mutate podSpec.
	var resourceRequirements v1.ResourceRequirements
	for _, container := range podSpec.Containers {
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

// CollapseContainerRequestsAndLimits collapses all containers and initContainers in podSpec into a single container.
// Each resource of the requests and limits of this container is set to
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
// This function may mutate containers in-place.
//
// TODO: Test.
func CollapsePodSpecContainers(podSpec *v1.PodSpec) {
	var container *v1.Container
	for _, c := range podSpec.Containers {
		if container == nil {
			container = &c
		} else {
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
	}
	for _, c := range podSpec.InitContainers {
		if container == nil {
			container = &c
		} else {
			for t, request := range c.Resources.Requests {
				q := container.Resources.Requests[t]
				if q.Cmp(request) == -1 {
					container.Resources.Requests[t] = request
				}
			}
			for t, limit := range c.Resources.Limits {
				q := container.Resources.Limits[t]
				if q.Cmp(limit) == -1 {
					container.Resources.Requests[t] = limit
				}
			}
		}
	}
	if len(podSpec.Containers) > 0 {
		podSpec.Containers = podSpec.Containers[0:1]
	} else if len(podSpec.InitContainers) > 0 {
		podSpec.Containers = podSpec.InitContainers[0:1]
	}
	podSpec.InitContainers = nil
}

// TotalPodResourceRequest represents the resource request for a given pod is the maximum of:
//   - sum of all containers
//   - any individual init container
//
// This is because:
//   - containers run in parallel (so need to sum resources)
//   - init containers run sequentially (so only their individual resource need be considered)
//
// So pod resource usage is the max for each resource type (cpu/memory etc.) that could be used at any given time
func PodTotalRequestsAsResourceList(podSpec *v1.PodSpec) schedulerobjects.ResourceList {
	totalRequests := make(map[string]resource.Quantity, 4)
	for _, container := range podSpec.Containers {
		for t, request := range container.Resources.Requests {
			q := totalRequests[string(t)]
			q.Add(request)
			totalRequests[string(t)] = q
		}
	}
	for _, container := range podSpec.InitContainers {
		for t, request := range container.Resources.Requests {
			q := totalRequests[string(t)]
			if q.Cmp(request) == -1 {
				totalRequests[string(t)] = q
			}
		}
	}
	return schedulerobjects.ResourceList{Resources: totalRequests}
}

// TotalPodResourceLimit function calculates the maximum total resource (cpu, memory, etc.) limits in the pod for
// each resource by iterating through all containers and initContainers in the pod.
func PodTotalLimitsAsResourceList(podSpec *v1.PodSpec) schedulerobjects.ResourceList {
	totalLimits := make(map[string]resource.Quantity, 4)
	for _, container := range podSpec.Containers {
		for t, request := range container.Resources.Limits {
			q := totalLimits[string(t)]
			q.Add(request)
			totalLimits[string(t)] = q
		}
	}
	for _, container := range podSpec.InitContainers {
		for t, request := range container.Resources.Limits {
			q := totalLimits[string(t)]
			if q.Cmp(request) == -1 {
				totalLimits[string(t)] = q
			}
		}
	}
	return schedulerobjects.ResourceList{Resources: totalLimits}
}

func PodTotalRequestsAsMillis(podSpec *v1.PodSpec) map[string]int64 {
	total := make(map[string]int64, 4)
	for _, container := range podSpec.Containers {
		for t, request := range container.Resources.Requests {
			total[string(t)] += request.MilliValue()
		}
	}
	for _, container := range podSpec.InitContainers {
		for t, request := range container.Resources.Requests {
			v := request.MilliValue()
			if v > total[string(t)] {
				total[string(t)] = v
			}
		}
	}
	return total
}

func PodTotalRequestsAsV1ResourceList3(podSpec *v1.PodSpec) v1.ResourceList {
	totalRequests := make(v1.ResourceList, 4)
	// buffer := resource.Quantity{}
	for _, container := range podSpec.Containers {
		for t, request := range container.Resources.Requests {
			// buffer.DeepCopyInto()
			q := totalRequests[t]
			q.Add(request)
			totalRequests[t] = q
		}
	}
	for _, container := range podSpec.InitContainers {
		for t, request := range container.Resources.Requests {
			q := totalRequests[t]
			if q.Cmp(request) == -1 {
				totalRequests[t] = q
			}
		}
	}
	return totalRequests
}

// TotalPodResourceRequest represents the resource request for a given pod is the maximum of:
//   - sum of all containers
//   - any individual init container
//
// This is because:
//   - containers run in parallel (so need to sum resources)
//   - init containers run sequentially (so only their individual resource need be considered)
//
// So pod resource usage is the max for each resource type (cpu/memory etc.) that could be used at any given time
func PodTotalRequestsAsV1ResourceList(podSpec *v1.PodSpec) v1.ResourceList {
	totalRequests := make(v1.ResourceList, 4)
	for _, container := range podSpec.Containers {
		for t, request := range container.Resources.Requests {
			q := totalRequests[t]
			q.Add(request)
			totalRequests[t] = q
		}
	}
	for _, container := range podSpec.InitContainers {
		for t, request := range container.Resources.Requests {
			q := totalRequests[t]
			if q.Cmp(request) == -1 {
				totalRequests[t] = q
			}
		}
	}
	return totalRequests
}

// TotalPodResourceLimit function calculates the maximum total resource (cpu, memory, etc.) limits in the pod for
// each resource by iterating through all containers and initContainers in the pod.
func PodTotalLimitsAsV1ResourceList(podSpec *v1.PodSpec) v1.ResourceList {
	totalLimits := make(v1.ResourceList, 4)
	for _, container := range podSpec.Containers {
		for t, request := range container.Resources.Limits {
			q := totalLimits[t]
			q.Add(request)
			totalLimits[t] = q
		}
	}
	for _, container := range podSpec.InitContainers {
		for t, request := range container.Resources.Limits {
			q := totalLimits[t]
			if q.Cmp(request) == -1 {
				totalLimits[t] = q
			}
		}
	}
	return totalLimits
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
