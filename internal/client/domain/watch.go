package domain

import (
	"fmt"
	"strings"

	"github.com/G-Research/armada/internal/armada/api"
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
	Status JobStatus
	Job    *api.Job
}

var statesToIncludeInSummary []JobStatus

// States where the job is still active, or might be active in the future:
var activeStates []JobStatus

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
	activeStates = []JobStatus{
		Queued,
		Leased,
		Pending,
		Running,
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
		info = &JobInfo{}
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

func (context *WatchContext) GetJobInfo(jobId string) JobInfo {
	return *context.state[jobId]
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

// Return whether there are any jobs in active states:
func (context *WatchContext) HasActiveJobs() bool {
	return context.GetNumberOfJobsInStates(activeStates) > 0
}

func updateJobInfo(info *JobInfo, event api.Event) {
	switch typed := event.(type) {
	case *api.JobSubmittedEvent:
		info.Status = Submitted
		info.Job = &typed.Job
	case *api.JobQueuedEvent:
		info.Status = Queued
	case *api.JobLeasedEvent:
		info.Status = Leased
	case *api.JobLeaseReturnedEvent:
		info.Status = Queued
	case *api.JobUnableToScheduleEvent:
		// NOOP
	case *api.JobLeaseExpiredEvent:
		info.Status = Queued
	case *api.JobPendingEvent:
		info.Status = Pending
	case *api.JobRunningEvent:
		info.Status = Running
	case *api.JobFailedEvent:
		info.Status = Failed
	case *api.JobSucceededEvent:
		info.Status = Succeeded
	case *api.JobReprioritizedEvent:
		// TODO
	case *api.JobTerminatedEvent:
		// NOOP
	case *api.JobCancelledEvent:
		info.Status = Cancelled
	}
}
