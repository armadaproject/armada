package hami

import (
	"slices"
	"sort"

	"github.com/armadaproject/armada/pkg/hamiapi"
)

// DeviceUsage is the amount of one device reserved by jobs.
type DeviceUsage struct {
	Slots       int64
	MemoryMiB   int64
	CorePercent int64
}

func (u DeviceUsage) exceeds(device *hamiapi.DeviceInfo) bool {
	return u.Slots > int64(device.SlotCount) || u.MemoryMiB > device.MemoryMib || u.CorePercent > int64(device.CorePercent)
}

// Usage is the amount reserved on each device, keyed by device id.
type Usage map[string]DeviceUsage

// Add records a reservation.
func (u Usage) Add(allocations []*hamiapi.DeviceAllocation) {
	for _, allocation := range allocations {
		usage := u[allocation.Id]
		usage.Slots++
		usage.MemoryMiB += allocation.MemoryMib
		usage.CorePercent += int64(allocation.CorePercent)
		u[allocation.Id] = usage
	}
}

// Oversubscribed reports whether usage exceeds the capacity of any device.
func (u Usage) Oversubscribed(devices []*hamiapi.DeviceInfo) bool {
	for _, device := range devices {
		if u[device.Id].exceeds(device) {
			return true
		}
	}
	return false
}

// SelectDevices chooses distinct usable devices on which the request fits
// given current usage, and returns the amount to reserve on each. Devices are
// chosen best-fit: those with the least capacity left after placement first.
func SelectDevices(devices []*hamiapi.DeviceInfo, usage Usage, request Request) ([]*hamiapi.DeviceAllocation, bool) {
	if !request.IsDeviceRequest() {
		return nil, true
	}
	type candidate struct {
		device    *hamiapi.DeviceInfo
		remaining DeviceUsage
	}
	cores := request.CoresPerDevice()
	candidates := make([]candidate, 0, len(devices))
	var memory int64
	for _, device := range devices {
		if !device.Usable {
			continue
		}
		// Devices are uniform, so the memory reserved is the same on each.
		memory = request.MemoryMiB
		if request.WholeMemory() {
			memory = device.MemoryMib
		}
		used := usage[device.Id]
		remaining := DeviceUsage{
			MemoryMiB:   device.MemoryMib - used.MemoryMiB - memory,
			CorePercent: int64(device.CorePercent) - used.CorePercent - cores,
			Slots:       int64(device.SlotCount) - used.Slots - 1,
		}
		if remaining.MemoryMiB < 0 || remaining.CorePercent < 0 || remaining.Slots < 0 {
			continue
		}
		candidates = append(candidates, candidate{device: device, remaining: remaining})
	}
	if int64(len(candidates)) < request.Devices {
		return nil, false
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i].remaining, candidates[j].remaining
		if a.MemoryMiB != b.MemoryMiB {
			return a.MemoryMiB < b.MemoryMiB
		}
		if a.CorePercent != b.CorePercent {
			return a.CorePercent < b.CorePercent
		}
		if a.Slots != b.Slots {
			return a.Slots < b.Slots
		}
		return candidates[i].device.Id < candidates[j].device.Id
	})

	result := make([]*hamiapi.DeviceAllocation, request.Devices)
	for i := range result {
		result[i] = &hamiapi.DeviceAllocation{
			Id:          candidates[i].device.Id,
			MemoryMib:   memory,
			CorePercent: int32(cores),
		}
	}
	return result, true
}

// FitsAllocations reports whether an existing reservation still fits on its
// devices given current usage by other jobs.
func FitsAllocations(devices []*hamiapi.DeviceInfo, usage Usage, allocations []*hamiapi.DeviceAllocation) bool {
	combined := Usage{}
	for id, u := range usage {
		combined[id] = u
	}
	combined.Add(allocations)
	for _, allocation := range allocations {
		i := slices.IndexFunc(devices, func(d *hamiapi.DeviceInfo) bool { return d.Id == allocation.Id })
		if i < 0 || combined[allocation.Id].exceeds(devices[i]) {
			return false
		}
	}
	return true
}
