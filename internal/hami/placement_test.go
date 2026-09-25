package hami

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/pkg/hamiapi"
)

func device(id string) *hamiapi.DeviceInfo {
	return &hamiapi.DeviceInfo{Id: id, SlotCount: 4, MemoryMib: 16384, CorePercent: 100, Mode: SupportedMode, Healthy: true, Usable: true}
}

func ids(allocations []*hamiapi.DeviceAllocation) []string {
	result := make([]string, len(allocations))
	for i, allocation := range allocations {
		result[i] = allocation.Id
	}
	return result
}

func TestSelectDevices(t *testing.T) {
	devices := []*hamiapi.DeviceInfo{device("gpu-a"), device("gpu-b"), device("gpu-c")}
	tests := map[string]struct {
		usage       Usage
		request     Request
		expectedIds []string
		expectedMiB int64
		expectFail  bool
	}{
		"fractional request packs onto the most used device": {
			usage:       Usage{"gpu-b": {Slots: 1, MemoryMiB: 8192, CorePercent: 50}},
			request:     Request{Devices: 1, MemoryMiB: 4096, CorePercent: 25},
			expectedIds: []string{"gpu-b"},
			expectedMiB: 4096,
		},
		"whole device reserves all memory of a free device": {
			usage:       Usage{"gpu-a": {Slots: 1, MemoryMiB: 1, CorePercent: 1}},
			request:     Request{Devices: 1},
			expectedIds: []string{"gpu-b"},
			expectedMiB: 16384,
		},
		"multi-GPU request uses distinct devices": {
			usage:       Usage{"gpu-b": {Slots: 1, MemoryMiB: 8192, CorePercent: 50}},
			request:     Request{Devices: 2, MemoryMiB: 8192, CorePercent: 50},
			expectedIds: []string{"gpu-b", "gpu-a"},
			expectedMiB: 8192,
		},
		"multi-GPU whole device fails without enough free devices": {
			usage:      Usage{"gpu-a": {Slots: 1, MemoryMiB: 1, CorePercent: 1}, "gpu-c": {Slots: 1, MemoryMiB: 1, CorePercent: 1}},
			request:    Request{Devices: 2},
			expectFail: true,
		},
		"fragmented capacity does not fit": {
			usage: Usage{
				"gpu-a": {Slots: 1, MemoryMiB: 8192}, "gpu-b": {Slots: 1, MemoryMiB: 8192}, "gpu-c": {Slots: 1, MemoryMiB: 8192},
			},
			request:    Request{Devices: 1, MemoryMiB: 9216},
			expectFail: true,
		},
		"no free slot": {
			usage:      Usage{"gpu-a": {Slots: 4}, "gpu-b": {Slots: 4}, "gpu-c": {Slots: 4}},
			request:    Request{Devices: 1, MemoryMiB: 1},
			expectFail: true,
		},
		"more devices than exist": {
			request:    Request{Devices: 4, MemoryMiB: 1},
			expectFail: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			usage := tc.usage
			if usage == nil {
				usage = Usage{}
			}
			allocations, ok := SelectDevices(devices, usage, tc.request)
			if tc.expectFail {
				assert.False(t, ok)
				return
			}
			require.True(t, ok)
			assert.Equal(t, tc.expectedIds, ids(allocations))
			for _, allocation := range allocations {
				assert.Equal(t, tc.expectedMiB, allocation.MemoryMib)
				assert.Equal(t, int32(tc.request.CoresPerDevice()), allocation.CorePercent)
			}
		})
	}
}

func TestSelectDevices_SkipsUnusableDevices(t *testing.T) {
	unusable := device("gpu-a")
	unusable.Usable = false
	allocations, ok := SelectDevices([]*hamiapi.DeviceInfo{unusable, device("gpu-b")}, Usage{}, Request{Devices: 1})
	require.True(t, ok)
	assert.Equal(t, []string{"gpu-b"}, ids(allocations))
}

func TestFitsAllocations(t *testing.T) {
	devices := []*hamiapi.DeviceInfo{device("gpu-a")}
	reservation := []*hamiapi.DeviceAllocation{{Id: "gpu-a", MemoryMib: 8192, CorePercent: 50}}
	assert.True(t, FitsAllocations(devices, Usage{"gpu-a": {Slots: 1, MemoryMiB: 8192, CorePercent: 50}}, reservation))
	assert.False(t, FitsAllocations(devices, Usage{"gpu-a": {Slots: 1, MemoryMiB: 8193, CorePercent: 50}}, reservation))
	assert.False(t, FitsAllocations(devices, Usage{}, []*hamiapi.DeviceAllocation{{Id: "gone", MemoryMib: 1}}))
}

func TestUsageOversubscribed(t *testing.T) {
	devices := []*hamiapi.DeviceInfo{device("gpu-a")}
	assert.False(t, Usage{"gpu-a": {Slots: 4, MemoryMiB: 16384, CorePercent: 100}}.Oversubscribed(devices))
	assert.True(t, Usage{"gpu-a": {Slots: 1, MemoryMiB: 16385}}.Oversubscribed(devices))
	assert.True(t, Usage{"gpu-a": {Slots: 1, CorePercent: 101}}.Oversubscribed(devices))
	assert.True(t, Usage{"gpu-a": {Slots: 5}}.Oversubscribed(devices))
}
