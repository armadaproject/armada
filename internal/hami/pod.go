package hami

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/pkg/hamiapi"
)

// UseGPUUUIDAnnotation pins a pod to specific physical GPUs. Armada owns it:
// it is written from the run's device reservation when serving the lease, and
// users may not set it.
const UseGPUUUIDAnnotation = "nvidia.com/use-gpuuuid"

// A job's pod has one primary container, the first regular container; any other
// containers are sidecars. A job's GPU request is its primary container's
// request, so only the primary container may request GPUs (submit validation
// enforces this) and a run's device reservation is written into it.

// RequestsGPUs reports whether a container requests GPUs or HAMi device amounts.
func RequestsGPUs(resources v1.ResourceRequirements) bool {
	for _, list := range []v1.ResourceList{resources.Requests, resources.Limits} {
		for _, name := range []v1.ResourceName{GPUResource, GPUMemoryResource, GPUCoreResource, GPUMemoryPercentageResource} {
			if _, ok := list[name]; ok {
				return true
			}
		}
	}
	return false
}

// ApplyAllocations writes a run's device reservation into its pod: the primary
// container gets explicit per-GPU memory and cores for the reserved devices, and
// the returned annotations pin the pod to them.
func ApplyAllocations(podSpec *v1.PodSpec, allocations []*hamiapi.DeviceAllocation) (map[string]string, error) {
	if len(allocations) == 0 {
		return nil, nil
	}
	if len(podSpec.Containers) == 0 {
		return nil, fmt.Errorf("cannot apply HAMi device reservation: pod has no containers")
	}
	container := &podSpec.Containers[0]

	ids := make([]string, len(allocations))
	for i, allocation := range allocations {
		ids[i] = allocation.Id
	}
	// Devices are uniform, so the amounts reserved are the same on each GPU.
	values := v1.ResourceList{
		GPUResource:       *resource.NewQuantity(int64(len(allocations)), resource.DecimalSI),
		GPUMemoryResource: *resource.NewQuantity(allocations[0].MemoryMib, resource.DecimalSI),
		GPUCoreResource:   *resource.NewQuantity(int64(allocations[0].CorePercent), resource.DecimalSI),
	}
	if container.Resources.Requests == nil {
		container.Resources.Requests = v1.ResourceList{}
	}
	if container.Resources.Limits == nil {
		container.Resources.Limits = v1.ResourceList{}
	}
	for name, quantity := range values {
		container.Resources.Requests[name] = quantity.DeepCopy()
		container.Resources.Limits[name] = quantity.DeepCopy()
	}
	delete(container.Resources.Requests, GPUMemoryPercentageResource)
	delete(container.Resources.Limits, GPUMemoryPercentageResource)
	return map[string]string{UseGPUUUIDAnnotation: strings.Join(ids, ",")}, nil
}
