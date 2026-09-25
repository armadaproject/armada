package hami

import (
	"encoding/json"

	"github.com/armadaproject/armada/pkg/hamiapi"
)

// registeredDevice is one entry of HAMi's hami.io/node-nvidia-register annotation.
type registeredDevice struct {
	ID       string `json:"id"`
	Count    int32  `json:"count"`
	DevMem   int64  `json:"devmem"`
	DevCores int32  `json:"devcore"`
	Type     string `json:"type"`
	Mode     string `json:"mode"`
	Health   bool   `json:"health"`
}

// NodeInventoryFromAnnotations validates the HAMi inventory registered on a
// node. It returns nil if the node has no HAMi registration.
func NodeInventoryFromAnnotations(annotations map[string]string) *hamiapi.NodeInventory {
	raw, ok := annotations[NodeNvidiaRegisterAnnotation]
	if !ok {
		return nil
	}
	return ParseNodeInventory(raw)
}

// ParseNodeInventory parses and validates the value of HAMi's
// hami.io/node-nvidia-register annotation.
func ParseNodeInventory(raw string) *hamiapi.NodeInventory {
	var registered []*registeredDevice
	if err := json.Unmarshal([]byte(raw), &registered); err != nil {
		return invalid("malformed device registration")
	}

	devices := make([]*hamiapi.DeviceInfo, 0, len(registered))
	seen := make(map[string]bool, len(registered))
	for _, r := range registered {
		if r == nil {
			continue
		}
		if r.ID == "" {
			return invalid("device with empty id")
		}
		if seen[r.ID] {
			return invalid("duplicate device id")
		}
		seen[r.ID] = true
		if r.Count < 0 || r.DevMem < 0 || r.DevCores < 0 {
			return invalid("negative device capacity")
		}
		if len(devices) > 0 && (r.DevMem != devices[0].MemoryMib || r.DevCores != devices[0].CorePercent) {
			return invalid("devices differ in memory or compute")
		}
		device := &hamiapi.DeviceInfo{
			Id:          r.ID,
			SlotCount:   r.Count,
			MemoryMib:   r.DevMem,
			CorePercent: r.DevCores,
			Type:        r.Type,
			Mode:        r.Mode,
			Healthy:     r.Health,
		}
		device.Usable = isUsable(device)
		devices = append(devices, device)
	}

	inventory := &hamiapi.NodeInventory{Devices: devices, Status: hamiapi.InventoryStatus_INVENTORY_STATUS_UNAVAILABLE}
	for _, device := range devices {
		if device.Usable {
			inventory.Status = hamiapi.InventoryStatus_INVENTORY_STATUS_USABLE
			return inventory
		}
	}
	inventory.Reason = "no healthy hami-core device"
	return inventory
}

// isUsable reports whether Armada can place onto a device: it must be healthy,
// run the supported mode and have the supported capacity units.
func isUsable(device *hamiapi.DeviceInfo) bool {
	return device.Healthy &&
		device.Mode == SupportedMode &&
		device.CorePercent == SupportedCoreCapacity &&
		device.SlotCount > 0 &&
		device.MemoryMib > 0
}

func invalid(reason string) *hamiapi.NodeInventory {
	return &hamiapi.NodeInventory{Status: hamiapi.InventoryStatus_INVENTORY_STATUS_INVALID, Reason: reason}
}

// IsRegistered reports whether a node is registered with HAMi, whatever the
// validity of its inventory. Such nodes are never ordinary GPU nodes.
func IsRegistered(inventory *hamiapi.NodeInventory) bool {
	return inventory != nil
}

// IsUsable reports whether a node has at least one device Armada can place onto.
func IsUsable(inventory *hamiapi.NodeInventory) bool {
	return inventory != nil && inventory.Status == hamiapi.InventoryStatus_INVENTORY_STATUS_USABLE
}
