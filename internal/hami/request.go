package hami

import (
	"fmt"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/pkg/hamiapi"
)

const (
	// maxDevices bounds the number of GPUs a single job can request.
	maxDevices = 1024
	// maxMemoryMiB bounds the memory a single job can request on one GPU (1 PiB).
	maxMemoryMiB = 1 << 30
)

// Request is a job's HAMi device request. Memory and cores are per GPU.
type Request struct {
	// Number of distinct GPUs requested.
	Devices int64
	// Memory requested on each GPU, in MiB. Zero means the whole device.
	MemoryMiB int64
	// Compute requested on each GPU, in percent. Zero means the whole device.
	CorePercent int64
}

// IsDeviceRequest reports whether the request asks for at least one GPU.
func (r Request) IsDeviceRequest() bool {
	return r.Devices > 0
}

// WholeMemory reports whether the request reserves all memory on each GPU.
func (r Request) WholeMemory() bool {
	return r.MemoryMiB == 0
}

// CoresPerDevice returns the compute reserved on each GPU, in percent.
func (r Request) CoresPerDevice() int64 {
	if r.CorePercent == 0 {
		return SupportedCoreCapacity
	}
	return r.CorePercent
}

// HasDeviceAmounts reports whether resources include HAMi per-GPU memory or
// cores, which only have meaning for placement onto HAMi GPUs.
func HasDeviceAmounts(resources v1.ResourceList) bool {
	for _, name := range []v1.ResourceName{GPUMemoryResource, GPUCoreResource, GPUMemoryPercentageResource} {
		if _, ok := resources[name]; ok {
			return true
		}
	}
	return false
}

// RequestFromResources extracts and validates a job's HAMi device request from
// its pod-level resource requests. Omitted memory or cores mean the whole
// device. An explicit value must be a positive integer.
func RequestFromResources(requests v1.ResourceList) (Request, error) {
	if _, ok := requests[GPUMemoryPercentageResource]; ok {
		return Request{}, fmt.Errorf("%s is not supported; request %s in MiB", GPUMemoryPercentageResource, GPUMemoryResource)
	}
	devices, hasDevices, err := integerQuantity(requests, GPUResource, maxDevices)
	if err != nil {
		return Request{}, err
	}
	memory, hasMemory, err := integerQuantity(requests, GPUMemoryResource, maxMemoryMiB)
	if err != nil {
		return Request{}, err
	}
	cores, hasCores, err := integerQuantity(requests, GPUCoreResource, SupportedCoreCapacity)
	if err != nil {
		return Request{}, err
	}
	if !hasDevices {
		if hasMemory || hasCores {
			return Request{}, fmt.Errorf("%s and %s require %s", GPUMemoryResource, GPUCoreResource, GPUResource)
		}
		return Request{}, nil
	}
	return Request{Devices: devices, MemoryMiB: memory, CorePercent: cores}, nil
}

// integerQuantity returns the value of a resource that, if present, must be an
// integer in [1, limit].
func integerQuantity(requests v1.ResourceList, name v1.ResourceName, limit int64) (int64, bool, error) {
	q, ok := requests[name]
	if !ok {
		return 0, false, nil
	}
	value, isInteger := q.AsInt64()
	if !isInteger {
		return 0, true, fmt.Errorf("%s must be an integer, got %s", name, q.String())
	}
	if value < 1 || value > limit {
		return 0, true, fmt.Errorf("%s must be between 1 and %d, got %d", name, limit, value)
	}
	return value, true, nil
}

// Charge returns the fair-share charge of a device request, summed across its
// GPUs. A placed request is charged the amounts reserved on its devices. An
// unplaced request is charged its explicit amounts, with omitted memory
// estimated as memoryEstimateMiB per GPU and omitted cores as the whole device.
func Charge(request Request, allocations []*hamiapi.DeviceAllocation, memoryEstimateMiB int64) (memoryMiB int64, corePercent int64) {
	if len(allocations) > 0 {
		for _, allocation := range allocations {
			memoryMiB += allocation.MemoryMib
			corePercent += int64(allocation.CorePercent)
		}
		return memoryMiB, corePercent
	}
	perDeviceMemory := request.MemoryMiB
	if request.WholeMemory() {
		perDeviceMemory = memoryEstimateMiB
	}
	return request.Devices * perDeviceMemory, request.Devices * request.CoresPerDevice()
}
