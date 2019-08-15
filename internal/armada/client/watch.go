package client

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/prometheus/common/log"
	"time"
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

func WatchJobSet(client api.EventClient, jobSetId string, onUpdate func(map[string]*JobInfo, api.Event)) {

	state := make(map[string]*JobInfo)

	for {
		clientStream, e := client.GetJobSetEvents(context.Background(), &api.JobSetRequest{Id: jobSetId, Watch: true})

		if e != nil {
			log.Error(e)
			time.Sleep(5 * time.Second)
			continue
		}

		for {

			msg, e := clientStream.Recv()
			if e != nil {
				log.Error(e)
				time.Sleep(5 * time.Second)
				break
			}

			event, e := api.UnwrapEvent(msg.Message)
			if e != nil {
				log.Error(e)
				time.Sleep(5 * time.Second)
				continue
			}

			info, exists := state[event.GetJobId()]
			if !exists {
				info = &JobInfo{}
				state[event.GetJobId()] = info
			}

			switch typed := event.(type) {
			case *api.JobSubmittedEvent:
				info.Status = Submitted
				info.Job = &typed.Job
			case *api.JobQueuedEvent:
				info.Status = Queued
			case *api.JobLeasedEvent:
				info.Status = Leased
			case *api.JobLeaseExpired:
				info.Status = Queued
			case *api.JobPendingEvent:
				info.Status = Pending
			case *api.JobRunningEvent:
				info.Status = Running
			case *api.JobFailedEvent:
				info.Status = Failed
				break
			case *api.JobSucceededEvent:
				info.Status = Succeeded
			case *api.JobReprioritizedEvent:
				// TODO
			case *api.JobCancellingEvent:
			case *api.JobCancelledEvent:
				info.Status = Cancelled

			}

			onUpdate(state, event)
		}
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
