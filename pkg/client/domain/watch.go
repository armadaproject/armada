package domain

import (
	"fmt"
	"strings"
	"time"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/pkg/api"
)

type (
	JobStatus string
	PodStatus string
)

const (
	Submitted = "Submitted"
	Duplicate = "Duplicate"
	Queued    = "Queued"
	Leased    = "Leased"
	Pending   = "Pending"
	Running   = "Running"
	Succeeded = "Succeeded"
	Failed    = "Failed"
	Cancelled = "Cancelled"
)

type JobInfo struct {
	Status           JobStatus
	PodStatus        []PodStatus
	Job              *api.Job
	LastUpdate       time.Time
	PodLastUpdated   []time.Time
	ClusterId        string
	MaxUsedResources armadaresource.ComputeResources
}

var statesToIncludeInSummary []JobStatus

// States where the job is finished:
var inactiveStates []JobStatus

func init() {
	statesToIncludeInSummary = []JobStatus{
		Queued,
		Leased,
		Pending,
		Running,
		Succeeded,
		Failed,
		Cancelled,
	}
	inactiveStates = []JobStatus{
		Succeeded,
		Failed,
		Cancelled,
	}
}

// WatchContext keeps track of the current state when processing a stream of events
// It is not threadsafe and is expected to only ever be used in a single thread
type WatchContext struct {
	state        map[string]*JobInfo
	stateSummary map[JobStatus]int
}

func NewWatchContext() *WatchContext {
	return &WatchContext{
		state:        make(map[string]*JobInfo, 10),
		stateSummary: make(map[JobStatus]int, 8),
	}
}

func (context *WatchContext) ProcessEvent(event api.Event) {
	info, exists := context.state[event.GetJobId()]
	if !exists {
		info = &JobInfo{
			MaxUsedResources: armadaresource.ComputeResources{},
		}
		context.state[event.GetJobId()] = info
	}

	currentJobStatus := info.Status

	updateJobInfo(info, event)
	context.updateStateSummary(currentJobStatus, info.Status)
}

func (context *WatchContext) updateStateSummary(oldJobStatus JobStatus, newJobStatus JobStatus) {
	if newJobStatus == "" {
		return
	}

	if oldJobStatus == newJobStatus {
		return
	}

	if oldJobStatus != "" {
		context.stateSummary[oldJobStatus]--
	}

	context.stateSummary[newJobStatus]++
}

func (context *WatchContext) GetJobInfo(jobId string) *JobInfo {
	return context.state[jobId]
}

func (context *WatchContext) GetCurrentState() map[string]*JobInfo {
	return context.state
}

func (context *WatchContext) GetCurrentStateSummary() string {
	first := true
	var summary strings.Builder

	for _, state := range statesToIncludeInSummary {
		if !first {
			summary.WriteString(", ")
		}
		first = false
		summary.WriteString(fmt.Sprintf("%s: %3d", state, context.stateSummary[state]))
	}

	return summary.String()
}

func (context *WatchContext) GetNumberOfJobsInStates(states []JobStatus) int {
	numberOfJobs := 0

	for _, state := range states {
		numberOfJobs += context.stateSummary[state]
	}

	return numberOfJobs
}

// Return number of finished jobs:
func (context *WatchContext) GetNumberOfFinishedJobs() int {
	return context.GetNumberOfJobsInStates(inactiveStates)
}

// Return number of jobs:
func (context *WatchContext) GetNumberOfJobs() int {
	numberOfJobs := 0

	for _, num := range context.stateSummary {
		numberOfJobs += num
	}

	return numberOfJobs
}

func (context *WatchContext) AreJobsFinished(ids []string) bool {
	for _, id := range ids {
		state, ok := context.state[id]

		if !ok || (state.Status != Succeeded &&
			state.Status != Failed &&
			state.Status != Cancelled) {
			return false
		}
	}
	return true
}

func updateJobInfo(info *JobInfo, event api.Event) {
	if isLifeCycleEvent(event) && !isPodEvent(event) {
		if info.LastUpdate.After(event.GetCreated()) {
			if submitEvent, ok := event.(*api.JobSubmittedEvent); ok {
				info.Job = &submitEvent.Job
			}
			// skipping event as it is out of time order
			return
		}
		info.LastUpdate = event.GetCreated()
	}

	switch typed := event.(type) {
	case *api.JobSubmittedEvent:
		info.Status = Submitted
		info.Job = &typed.Job
		for len(info.PodStatus) < len(typed.Job.PodSpecs) {
			info.PodStatus = append(info.PodStatus, Submitted)
			info.PodLastUpdated = append(info.PodLastUpdated, time.Time{})
		}
	case *api.JobDuplicateFoundEvent:
		info.Status = Duplicate
	case *api.JobQueuedEvent:
		info.Status = Queued
	case *api.JobLeasedEvent:
		info.Status = Leased
		info.ClusterId = typed.ClusterId
	case *api.JobLeaseReturnedEvent:
		info.Status = Queued
		resetPodStatus(info)
	case *api.JobLeaseExpiredEvent:
		info.Status = Queued
		resetPodStatus(info)
	case *api.JobCancelledEvent:
		info.Status = Cancelled

	// pod events:
	case *api.JobPendingEvent:
		updatePodStatus(info, typed, Pending)
	case *api.JobRunningEvent:
		updatePodStatus(info, typed, Running)
	case *api.JobFailedEvent:
		updatePodStatus(info, typed, Failed)
	case *api.JobSucceededEvent:
		updatePodStatus(info, typed, Succeeded)

	case *api.JobUnableToScheduleEvent:
		// NOOP
	case *api.JobReprioritizingEvent:
		// TODO
	case *api.JobReprioritizedEvent:
		// TODO
	case *api.JobTerminatedEvent:
		// NOOP
	case *api.JobIngressInfoEvent:
		// NOOP
	case *api.JobUtilisationEvent:
		info.MaxUsedResources.Max(typed.MaxResourcesForPeriod)
	case *api.JobUpdatedEvent:
		info.Job = &typed.Job
	}
}

func resetPodStatus(info *JobInfo) {
	for i := 0; i < len(info.PodStatus); i++ {
		info.PodStatus[i] = Submitted
		info.PodLastUpdated[i] = time.Time{}
	}
}

func updatePodStatus(info *JobInfo, event api.KubernetesEvent, status PodStatus) {
	info.ClusterId = event.GetClusterId()
	podNumber := event.GetPodNumber()

	for len(info.PodStatus) <= int(podNumber) {
		info.PodStatus = append(info.PodStatus, Submitted)
		info.PodLastUpdated = append(info.PodLastUpdated, time.Time{})
	}
	if info.PodLastUpdated[podNumber].After(event.GetCreated()) {
		// skipping event as it is out of time order
		return
	}
	info.PodLastUpdated[podNumber] = event.GetCreated()
	info.PodStatus[podNumber] = status

	//if info.Status == Cancelled {
	//	// cancelled is final state
	//	return
	//}

	stateCounts := map[PodStatus]uint{}
	lastPodUpdate := time.Time{}
	for i, s := range info.PodStatus {
		stateCounts[s]++
		if info.PodLastUpdated[i].After(lastPodUpdate) {
			lastPodUpdate = info.PodLastUpdated[i]
		}
	}

	if info.LastUpdate.After(lastPodUpdate) {
		// job state is newer than all pod updates
		return
	}
	info.LastUpdate = lastPodUpdate

	if stateCounts[Failed] > 0 {
		info.Status = Failed
	} else if stateCounts[Pending] > 0 {
		info.Status = Pending
	} else if stateCounts[Running] > 0 {
		info.Status = Running
	} else {
		info.Status = Succeeded
	}
}

func isLifeCycleEvent(event api.Event) bool {
	switch event.(type) {
	case *api.JobSubmittedEvent:
		return true
	case *api.JobQueuedEvent:
		return true
	case *api.JobDuplicateFoundEvent:
		return true
	case *api.JobLeasedEvent:
		return true
	case *api.JobLeaseReturnedEvent:
		return true
	case *api.JobLeaseExpiredEvent:
		return true

	case *api.JobPendingEvent:
		return true
	case *api.JobRunningEvent:
		return true
	case *api.JobFailedEvent:
		return true
	case *api.JobSucceededEvent:
		return true

	case *api.JobCancelledEvent:
		return true
	case *api.JobUnableToScheduleEvent:
		return false
	case *api.JobReprioritizedEvent:
		return false
	case *api.JobTerminatedEvent:
		return false
	case *api.JobUtilisationEvent:
		return false
	default:
		return false
	}
}

func isPodEvent(event api.Event) bool {
	switch event.(type) {
	case *api.JobPendingEvent:
		return true
	case *api.JobRunningEvent:
		return true
	case *api.JobFailedEvent:
		return true
	case *api.JobSucceededEvent:
		return true
	default:
		return false
	}
}
