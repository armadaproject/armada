package nodedb

import (
	"github.com/segmentio/fasthash/fnv1a"

	"github.com/armadaproject/armada/internal/hami"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/pkg/hamiapi"
)

// HamiRequirementNotMet is why a job requesting GPUs cannot be placed onto a
// node's HAMi GPUs. Reasons are bounded, so they aggregate across nodes.
type HamiRequirementNotMet struct {
	Reason string
}

func (r *HamiRequirementNotMet) Sum64() uint64 {
	return fnv1a.AddString64(fnv1a.AddString64(fnv1a.Init64, "HamiRequirementNotMet"), r.Reason)
}

func (r *HamiRequirementNotMet) String() string {
	return r.Reason
}

var insufficientHamiDevices = &HamiRequirementNotMet{Reason: "insufficient HAMi GPU capacity"}

// HamiRequirementsMet checks whether a job's GPU request is compatible with a
// node, independently of the node's current GPU usage.
//
// In a HAMi pool, GPU jobs are placed only onto usable HAMi nodes, and only if
// they meet the HAMi pod contract. Outside a HAMi pool, GPU jobs are never
// placed onto nodes registered with HAMi, since HAMi would place them onto GPUs
// Armada does not track, and HAMi device resources cannot be requested.
func HamiRequirementsMet(node *internaltypes.Node, jctx *schedulercontext.JobSchedulingContext) (bool, PodRequirementsNotMetReason) {
	job := jctx.Job
	if job == nil || !job.RequestsHamiDevices() {
		return true, nil
	}
	inventory := node.HamiInventory()
	if !node.IsHamiPool() {
		if hami.IsRegistered(inventory) {
			return false, &HamiRequirementNotMet{Reason: "node is registered with HAMi but its pool does not place onto HAMi GPUs"}
		}
		if hami.HasDeviceAmounts(jctx.PodRequirements.ResourceRequirements.Requests) {
			return false, &HamiRequirementNotMet{Reason: "HAMi GPU memory or cores requested outside a HAMi pool"}
		}
		return true, nil
	}
	if _, err := job.HamiRequest(); err != nil {
		return false, &HamiRequirementNotMet{Reason: "invalid HAMi device request"}
	}
	switch {
	case inventory == nil:
		return false, &HamiRequirementNotMet{Reason: "node is not registered with HAMi"}
	case inventory.Status == hamiapi.InventoryStatus_INVENTORY_STATUS_INVALID:
		return false, &HamiRequirementNotMet{Reason: "node HAMi inventory is invalid"}
	case inventory.Status != hamiapi.InventoryStatus_INVENTORY_STATUS_USABLE:
		return false, &HamiRequirementNotMet{Reason: "node has no usable HAMi GPU"}
	}
	return true, nil
}

// hamiPlacement chooses the HAMi GPUs for a job on a node from the point of
// view of a job scheduled at priority, with or without urgency preemption (see
// Node.HamiDeviceUsage). It returns nil allocations if the job needs no HAMi
// GPUs on this node. A job already bound to the node keeps the GPUs it reserved
// when it was first placed, if they still fit.
func hamiPlacement(
	node *internaltypes.Node,
	jctx *schedulercontext.JobSchedulingContext,
	priority int32,
	urgency bool,
) ([]*hamiapi.DeviceAllocation, bool, PodRequirementsNotMetReason) {
	job := jctx.Job
	if job == nil || !node.IsHamiPool() || !job.RequestsHamiDevices() {
		return nil, true, nil
	}
	devices := node.HamiInventory().GetDevices()
	jobId := job.Id()
	if node.HasJobAllocation(jobId) {
		// Rescheduling a run already bound to this node: its pod is running on
		// the GPUs it reserved, so it may only stay if they still fit.
		existing := node.HamiDeviceAllocations(jobId)
		if len(existing) == 0 {
			return nil, true, nil
		}
		if !hami.FitsAllocations(devices, node.HamiDeviceUsage(priority, urgency, jobId), existing) {
			return nil, false, insufficientHamiDevices
		}
		return existing, true, nil
	}
	request, err := job.HamiRequest()
	if err != nil {
		return nil, false, &HamiRequirementNotMet{Reason: "invalid HAMi device request"}
	}
	allocations, ok := hami.SelectDevices(devices, node.HamiDeviceUsage(priority, urgency, ""), request)
	if !ok {
		return nil, false, insufficientHamiDevices
	}
	return allocations, true, nil
}
