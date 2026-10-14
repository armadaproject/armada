package hami

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/pkg/hamiapi"
)

func TestParseNodeInventory(t *testing.T) {
	tests := map[string]struct {
		raw            string
		expectedStatus hamiapi.InventoryStatus
		expectedUsable []bool
	}{
		"usable devices": {
			raw:            `[{"id":"gpu-0","count":4,"devmem":16384,"devcore":100,"type":"A10","mode":"hami-core","health":true},{"id":"gpu-1","count":4,"devmem":16384,"devcore":100,"mode":"hami-core","health":true}]`,
			expectedStatus: hamiapi.InventoryStatus_INVENTORY_STATUS_USABLE,
			expectedUsable: []bool{true, true},
		},
		"unhealthy and unsupported devices are listed but not usable": {
			raw: `[{"id":"gpu-0","count":4,"devmem":16384,"devcore":100,"mode":"hami-core","health":false},` +
				`{"id":"gpu-1","count":4,"devmem":16384,"devcore":100,"mode":"mig","health":true},` +
				`{"id":"gpu-2","count":4,"devmem":16384,"devcore":100,"mode":"hami-core","health":true}]`,
			expectedStatus: hamiapi.InventoryStatus_INVENTORY_STATUS_USABLE,
			expectedUsable: []bool{false, false, true},
		},
		"no usable device": {
			raw:            `[{"id":"gpu-0","count":4,"devmem":16384,"devcore":100,"mode":"mps","health":true}]`,
			expectedStatus: hamiapi.InventoryStatus_INVENTORY_STATUS_UNAVAILABLE,
			expectedUsable: []bool{false},
		},
		"empty mode is not supported": {
			raw:            `[{"id":"gpu-0","count":4,"devmem":16384,"devcore":100,"health":true}]`,
			expectedStatus: hamiapi.InventoryStatus_INVENTORY_STATUS_UNAVAILABLE,
			expectedUsable: []bool{false},
		},
		"scaled core capacity is not supported": {
			raw:            `[{"id":"gpu-0","count":4,"devmem":16384,"devcore":200,"mode":"hami-core","health":true}]`,
			expectedStatus: hamiapi.InventoryStatus_INVENTORY_STATUS_UNAVAILABLE,
			expectedUsable: []bool{false},
		},
		"no devices": {
			raw:            `[]`,
			expectedStatus: hamiapi.InventoryStatus_INVENTORY_STATUS_UNAVAILABLE,
			expectedUsable: []bool{},
		},
		"malformed": {
			raw:            `GPU-0,10,32768,100,NVIDIA,0,true:`,
			expectedStatus: hamiapi.InventoryStatus_INVENTORY_STATUS_INVALID,
		},
		"duplicate ids": {
			raw:            `[{"id":"gpu-0","count":4,"devmem":1,"devcore":100,"mode":"hami-core","health":true},{"id":"gpu-0","count":4,"devmem":1,"devcore":100,"mode":"hami-core","health":true}]`,
			expectedStatus: hamiapi.InventoryStatus_INVENTORY_STATUS_INVALID,
		},
		"empty id": {
			raw:            `[{"id":"","count":4,"devmem":1,"devcore":100,"mode":"hami-core","health":true}]`,
			expectedStatus: hamiapi.InventoryStatus_INVENTORY_STATUS_INVALID,
		},
		"negative capacity": {
			raw:            `[{"id":"gpu-0","count":4,"devmem":-1,"devcore":100,"mode":"hami-core","health":true}]`,
			expectedStatus: hamiapi.InventoryStatus_INVENTORY_STATUS_INVALID,
		},
		"devices of different sizes": {
			raw:            `[{"id":"gpu-0","count":4,"devmem":16384,"devcore":100,"mode":"hami-core","health":true},{"id":"gpu-1","count":4,"devmem":81920,"devcore":100,"mode":"hami-core","health":true}]`,
			expectedStatus: hamiapi.InventoryStatus_INVENTORY_STATUS_INVALID,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			inventory := ParseNodeInventory(tc.raw)
			assert.Equal(t, tc.expectedStatus, inventory.Status)
			assert.True(t, IsRegistered(inventory))
			assert.Equal(t, tc.expectedStatus == hamiapi.InventoryStatus_INVENTORY_STATUS_USABLE, IsUsable(inventory))
			if tc.expectedStatus != hamiapi.InventoryStatus_INVENTORY_STATUS_USABLE {
				assert.NotEmpty(t, inventory.Reason)
			}
			if tc.expectedUsable != nil {
				usable := make([]bool, len(inventory.Devices))
				for i, device := range inventory.Devices {
					usable[i] = device.Usable
				}
				assert.Equal(t, tc.expectedUsable, usable)
			}
		})
	}
}

func TestNodeInventoryFromAnnotations_Unregistered(t *testing.T) {
	inventory := NodeInventoryFromAnnotations(map[string]string{"other": "value"})
	assert.Nil(t, inventory)
	assert.False(t, IsRegistered(inventory))
	assert.False(t, IsUsable(inventory))
}
