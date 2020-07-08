package domain

import (
	"fmt"
	"strings"
	"time"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
)

type JobStatus string

const (
	Submitted = "Submitted"
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
	Job              *api.Job
	LastUpdate       time.Time
	ClusterId        string
	MaxUsedResources common.ComputeResources
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

//WatchContext keeps track of the current state when processing a stream of events
//It is not threadsafe and is expected to only ever be used in a single thread
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
			MaxUsedResources: common.ComputeResources{},
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
	if isLifeCycleEvent(event) {
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
	case *api.JobQueuedEvent:
		info.Status = Queued
	case *api.JobLeasedEvent:
		info.Status = Leased
		info.ClusterId = typed.ClusterId
	case *api.JobLeaseReturnedEvent:
		info.Status = Queued
	case *api.JobLeaseExpiredEvent:
		info.Status = Queued
	case *api.JobPendingEvent:
		info.Status = Pending
		info.ClusterId = typed.ClusterId
	case *api.JobRunningEvent:
		info.Status = Running
		info.ClusterId = typed.ClusterId
	case *api.JobFailedEvent:
		info.Status = Failed
		info.ClusterId = typed.ClusterId
	case *api.JobSucceededEvent:
		info.Status = Succeeded
		info.ClusterId = typed.ClusterId
	case *api.JobCancelledEvent:
		info.Status = Cancelled

	case *api.JobUnableToScheduleEvent:
		// NOOP
	case *api.JobReprioritizedEvent:
		// TODO
	case *api.JobTerminatedEvent:
		// NOOP
	case *api.JobUtilisationEvent:
		info.MaxUsedResources.Max(typed.MaxResourcesForPeriod)
	}
}

func isLifeCycleEvent(event api.Event) bool {
	switch event.(type) {
	case *api.JobSubmittedEvent:
		return true
	case *api.JobQueuedEvent:
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
