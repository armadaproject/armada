// Package hami contains Armada's integration with HAMi's NVIDIA hami-core
// backend: the resource and annotation contract, inventory validation and
// physical-device placement.
//
// A node's GPUs are assumed to be uniform: the same memory and compute on every
// device. Inventory validation enforces this, so placement never has to choose
// between device sizes and a request's per-GPU amounts apply to every GPU it is
// placed on. A job's GPU request is that of its primary container, the first
// regular container of its pod (see pod.go).
package hami

const (
	// GPUResource is the number of HAMi slots requested, one per physical GPU.
	GPUResource = "nvidia.com/gpu"
	// GPUMemoryResource is the memory requested on each GPU, in MiB.
	GPUMemoryResource = "nvidia.com/gpumem"
	// GPUCoreResource is the compute requested on each GPU, in percent.
	GPUCoreResource = "nvidia.com/gpucores"
	// GPUMemoryPercentageResource is HAMi's percentage memory request. Armada
	// always resolves memory to an explicit MiB value, so it is not supported.
	GPUMemoryPercentageResource = "nvidia.com/gpumem-percentage"

	// NodeNvidiaRegisterAnnotation is written by HAMi's device plugin and holds
	// the node's NVIDIA device inventory.
	NodeNvidiaRegisterAnnotation = "hami.io/node-nvidia-register"

	// SupportedMode is the only HAMi device mode Armada places onto.
	SupportedMode = "hami-core"
	// SupportedCoreCapacity is the only per-device compute capacity Armada supports.
	SupportedCoreCapacity = 100
)

// IsDeviceResource reports whether a resource is reserved per physical GPU
// rather than per node.
func IsDeviceResource(name string) bool {
	return name == GPUMemoryResource || name == GPUCoreResource
}
