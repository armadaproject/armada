package jobdb

import (
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/hami"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/pkg/hamiapi"
)

func hamiRequestFromSchedulingInfo(schedulingInfo *internaltypes.JobSchedulingInfo) (hami.Request, error) {
	if schedulingInfo == nil || schedulingInfo.PodRequirements == nil {
		return hami.Request{}, nil
	}
	return hami.RequestFromResources(schedulingInfo.PodRequirements.ResourceRequirements.Requests)
}

// HamiRequest returns the job's HAMi device request, or an error if the job's
// device resources are invalid.
func (job *Job) HamiRequest() (hami.Request, error) {
	return job.hamiRequest, job.hamiRequestErr
}

// RequestsHamiDevices reports whether the job requests GPUs. An invalid request
// counts as one so that the job is refused by every GPU node, including
// ordinary ones, rather than placed onto a GPU Armada cannot account for.
func (job *Job) RequestsHamiDevices() bool {
	return job.hamiRequestErr != nil || job.hamiRequest.IsDeviceRequest()
}

// ActiveHamiDeviceAllocations returns the HAMi GPUs reserved for the job's
// active run, or nil if the job is queued or its latest run has finished.
func (job *Job) ActiveHamiDeviceAllocations() []*hamiapi.DeviceAllocation {
	if job.queued {
		return nil
	}
	run := job.LatestRun()
	if run == nil || run.InTerminalState() {
		return nil
	}
	return run.HamiDeviceAllocations()
}

// WithHamiCharge returns a view of the job whose device resource requirements
// are its fair-share charge in a pool whose mean GPU memory is
// memoryEstimateMiB. A job whose active run has reserved GPUs is charged the
// reserved amounts; otherwise omitted memory is charged at the estimate.
// Kubernetes resource requirements, pod requirements and the scheduling key are
// unchanged, so the view can be stored back in the job db.
func (job *Job) WithHamiCharge(memoryEstimateMiB int64) *Job {
	if job.hamiRequestErr != nil || !job.hamiRequest.IsDeviceRequest() {
		return job
	}
	memoryMiB, corePercent := hami.Charge(job.hamiRequest, job.ActiveHamiDeviceAllocations(), memoryEstimateMiB)
	return job.withDeviceCharge(memoryMiB, corePercent)
}

// WithHamiChargeForAllocations returns a view of the job charged for the given
// GPU reservation, e.g. once a candidate placement has been chosen.
func (job *Job) WithHamiChargeForAllocations(allocations []*hamiapi.DeviceAllocation) *Job {
	if len(allocations) == 0 {
		return job
	}
	memoryMiB, corePercent := hami.Charge(job.hamiRequest, allocations, 0)
	return job.withDeviceCharge(memoryMiB, corePercent)
}

// WithHamiDeviceAllocations returns a copy of the job whose latest run has
// reserved the given GPUs, charged for that reservation.
func (job *Job) WithHamiDeviceAllocations(allocations []*hamiapi.DeviceAllocation) (*Job, error) {
	run := job.LatestRun()
	if run == nil {
		return nil, errors.Errorf("cannot reserve HAMi devices for job %s: job has no run", job.id)
	}
	return job.WithUpdatedRun(run.WithHamiDeviceAllocations(allocations)).WithHamiChargeForAllocations(allocations), nil
}

func (job *Job) withDeviceCharge(memoryMiB, corePercent int64) *Job {
	charge := job.jobDb.resourceListFactory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{
		hami.GPUMemoryResource: *resource.NewQuantity(memoryMiB, resource.DecimalSI),
		hami.GPUCoreResource:   *resource.NewQuantity(corePercent, resource.DecimalSI),
	}).OfType(internaltypes.Device)
	current := job.allResourceRequirements.OfType(internaltypes.Device)
	if charge.Equal(current) {
		return job
	}
	j := shallowCopyJob(*job)
	j.allResourceRequirements = job.allResourceRequirements.Subtract(current).Add(charge)
	return j
}
