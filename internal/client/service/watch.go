package service

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/common/log"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/common/util"
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
}

func WatchJobSet(client api.EventClient, jobSetId string, waitForNew bool, context context.Context, onUpdate func(map[string]*JobInfo, api.Event) bool) {
	WatchJobSetWithJobIdsFilter(client, jobSetId, waitForNew, []string{}, context, onUpdate)
}

func WatchJobSetWithJobIdsFilter(client api.EventClient, jobSetId string, waitForNew bool, jobIds []string, context context.Context, onUpdate func(map[string]*JobInfo, api.Event) bool) {
	state := make(map[string]*JobInfo)

	jobIdsSet := util.StringListToSet(jobIds)
	filterOnJobId := len(jobIdsSet) > 0
	lastMessageId := ""

	for {
		select {
		case <-context.Done():
			return
		default:
		}

		clientStream, e := client.GetJobSetEvents(context, &api.JobSetRequest{Id: jobSetId, FromMessageId: lastMessageId, Watch: waitForNew})

		if e != nil {
			log.Error(e)
			time.Sleep(5 * time.Second)
			continue
		}

		receivedThisCall := 0

		for {

			msg, e := clientStream.Recv()
			if e != nil {
				log.Error(e)
				time.Sleep(5 * time.Second)
				break
			}
			receivedThisCall++
			lastMessageId = msg.Id

			event, e := api.UnwrapEvent(msg.Message)
			if e != nil {
				log.Error(e)
				time.Sleep(5 * time.Second)
				continue
			}

			if filterOnJobId && !jobIdsSet[event.GetJobId()] {
				continue
			}

			info, exists := state[event.GetJobId()]
			if !exists {
				info = &JobInfo{}
				state[event.GetJobId()] = info
			}

			updateJobInfo(info, event)

			shouldExit := onUpdate(state, event)
			if shouldExit {
				return
			}
		}

		if receivedThisCall == 0 && !waitForNew {
			return
		}
	}
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

func CountStates(state map[string]*JobInfo) map[JobStatus]int {
	result := map[JobStatus]int{}
	for _, jobInfo := range state {
		count, _ := result[jobInfo.Status]
		result[jobInfo.Status] = count + 1
	}
	return result
}

func CreateSummaryOfCurrentState(state map[string]*JobInfo) string {
	counts := CountStates(state)

	first := true
	summary := ""

	for _, state := range statesToIncludeInSummary {
		if !first {
			summary += ", "
		}
		first = false
		summary += fmt.Sprintf("%s: %3d", state, counts[state])
	}

	return summary
}
