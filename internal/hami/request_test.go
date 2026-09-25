package hami

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/pkg/hamiapi"
)

func resources(values map[string]string) v1.ResourceList {
	result := v1.ResourceList{}
	for name, value := range values {
		result[v1.ResourceName(name)] = resource.MustParse(value)
	}
	return result
}

func TestRequestFromResources(t *testing.T) {
	tests := map[string]struct {
		requests  map[string]string
		expected  Request
		expectErr bool
	}{
		"no gpu":               {requests: map[string]string{"cpu": "1"}},
		"whole device":         {requests: map[string]string{GPUResource: "2"}, expected: Request{Devices: 2}},
		"explicit amounts":     {requests: map[string]string{GPUResource: "1", GPUMemoryResource: "4096", GPUCoreResource: "25"}, expected: Request{Devices: 1, MemoryMiB: 4096, CorePercent: 25}},
		"explicit memory":      {requests: map[string]string{GPUResource: "1", GPUMemoryResource: "4096"}, expected: Request{Devices: 1, MemoryMiB: 4096}},
		"memory without gpu":   {requests: map[string]string{GPUMemoryResource: "4096"}, expectErr: true},
		"cores without gpu":    {requests: map[string]string{GPUCoreResource: "25"}, expectErr: true},
		"explicit zero memory": {requests: map[string]string{GPUResource: "1", GPUMemoryResource: "0"}, expectErr: true},
		"explicit zero cores":  {requests: map[string]string{GPUResource: "1", GPUCoreResource: "0"}, expectErr: true},
		"cores above 100":      {requests: map[string]string{GPUResource: "1", GPUCoreResource: "101"}, expectErr: true},
		"fractional gpu":       {requests: map[string]string{GPUResource: "500m"}, expectErr: true},
		"binary memory suffix": {requests: map[string]string{GPUResource: "1", GPUMemoryResource: "10Gi"}, expectErr: true},
		"huge gpu count":       {requests: map[string]string{GPUResource: "1000000000"}, expectErr: true},
		"memory percentage":    {requests: map[string]string{GPUResource: "1", GPUMemoryPercentageResource: "50"}, expectErr: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			request, err := RequestFromResources(resources(tc.requests))
			if tc.expectErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, request)
		})
	}
}

func TestCharge(t *testing.T) {
	tests := map[string]struct {
		request        Request
		allocations    []*hamiapi.DeviceAllocation
		estimate       int64
		expectedMemory int64
		expectedCores  int64
	}{
		"omitted memory is charged at the estimate per device": {
			request:        Request{Devices: 2},
			estimate:       49152,
			expectedMemory: 2 * 49152,
			expectedCores:  200,
		},
		"explicit amounts are charged per device": {
			request:        Request{Devices: 2, MemoryMiB: 4096, CorePercent: 25},
			estimate:       49152,
			expectedMemory: 8192,
			expectedCores:  50,
		},
		"a reservation is charged its reserved amounts": {
			request: Request{Devices: 2},
			allocations: []*hamiapi.DeviceAllocation{
				{Id: "gpu-0", MemoryMib: 16384, CorePercent: 100},
				{Id: "gpu-1", MemoryMib: 16384, CorePercent: 100},
			},
			estimate:       49152,
			expectedMemory: 32768,
			expectedCores:  200,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			memory, cores := Charge(tc.request, tc.allocations, tc.estimate)
			assert.Equal(t, tc.expectedMemory, memory)
			assert.Equal(t, tc.expectedCores, cores)
		})
	}
}
