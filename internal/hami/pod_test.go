package hami

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/pkg/hamiapi"
)

func container(name string, requests map[string]string) v1.Container {
	list := resources(requests)
	return v1.Container{Name: name, Resources: v1.ResourceRequirements{Requests: list, Limits: list.DeepCopy()}}
}

func TestApplyAllocations(t *testing.T) {
	spec := v1.PodSpec{Containers: []v1.Container{
		container("main", map[string]string{"cpu": "1", GPUResource: "2"}),
		container("sidecar", map[string]string{"cpu": "1"}),
	}}
	annotations, err := ApplyAllocations(&spec, []*hamiapi.DeviceAllocation{
		{Id: "gpu-a", MemoryMib: 81920, CorePercent: 100},
		{Id: "gpu-b", MemoryMib: 81920, CorePercent: 100},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{UseGPUUUIDAnnotation: "gpu-a,gpu-b"}, annotations)
	main := spec.Containers[0].Resources
	for _, list := range []v1.ResourceList{main.Requests, main.Limits} {
		gpus, memory, cores := list[GPUResource], list[GPUMemoryResource], list[GPUCoreResource]
		assert.Equal(t, int64(2), gpus.Value())
		assert.Equal(t, int64(81920), memory.Value())
		assert.Equal(t, int64(100), cores.Value())
	}
	_, sidecarHasGPU := spec.Containers[1].Resources.Requests[GPUResource]
	assert.False(t, sidecarHasGPU)

	_, err = ApplyAllocations(&v1.PodSpec{}, []*hamiapi.DeviceAllocation{{Id: "gpu-a", MemoryMib: 1}})
	assert.Error(t, err)
}
