package server

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/pkg/api"
)

func reportQueued(repository repository.EventStore, jobs []*api.Job) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, err := api.Wrap(&api.JobQueuedEvent{
			JobId:    job.Id,
			Queue:    job.Queue,
			JobSetId: job.JobSetId,
			Created:  now,
		})
		if err != nil {
			return fmt.Errorf("[reportQueued] error wrapping event: %w", err)
		}
		events = append(events, event)
	}

	err := repository.ReportEvents(events)
	if err != nil {
		return fmt.Errorf("[reportQueued] error reporting events: %w", err)
	}

	return nil
}

func reportDuplicateDetected(repository repository.EventStore, results []*repository.SubmitJobResult) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, result := range results {
		event, err := api.Wrap(&api.JobDuplicateFoundEvent{
			JobId:         result.SubmittedJob.Id,
			Queue:         result.SubmittedJob.Queue,
			JobSetId:      result.SubmittedJob.JobSetId,
			Created:       now,
			OriginalJobId: result.JobId,
		})
		if err != nil {
			return fmt.Errorf("[reportDuplicateDetected] error wrapping event: %w", err)
		}
		events = append(events, event)
	}

	err := repository.ReportEvents(events)
	if err != nil {
		return fmt.Errorf("[reportDuplicateDetected] error reporting events: %w", err)
	}

	return nil
}

//func reportDuplicateFoundEvents(repository repository.EventStore, events []*api.JobDuplicateFoundEvent) error {
//	apiEvents := make([]*api.EventMessage, len(events))
//	for i, event := range events {
//		event, err := api.Wrap(event)
//		if err != nil {
//			return errors.WithStack(err)
//		}
//		apiEvents[i] = event
//	}
//	err := repository.ReportEvents(apiEvents)
//	if err != nil {
//		return errors.WithStack(err)
//	}
//	return nil
//}

func reportSubmitted(repository repository.EventStore, jobs []*api.Job) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, err := api.Wrap(&api.JobSubmittedEvent{
			JobId:    job.Id,
			Queue:    job.Queue,
			JobSetId: job.JobSetId,
			Created:  now,
			Job:      *job,
		})
		if err != nil {
			return fmt.Errorf("[reportSubmitted] error wrapping event: %w", err)
		}
		events = append(events, event)
	}

	err := repository.ReportEvents(events)
	if err != nil {
		return fmt.Errorf("[reportSubmitted] error reporting events: %w", err)
	}

	return nil
}

// TODO This function behaves differently from the rest in this file.
// We should consolidate so that they all behave in the same way.
func reportJobsLeased(repository repository.EventStore, jobs []*api.Job, clusterId string) {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, err := api.Wrap(&api.JobLeasedEvent{
			JobId:     job.Id,
			Queue:     job.Queue,
			JobSetId:  job.JobSetId,
			Created:   now,
			ClusterId: clusterId,
		})
		if err != nil {
			err = fmt.Errorf("[reportJobsLeased] error wrapping event: %w", err)
			log.Error(err)
		} else {
			events = append(events, event)
		}
	}

	err := repository.ReportEvents(events)
	if err != nil {
		err = fmt.Errorf("[reportJobsLeased] error reporting events: %w", err)
		log.Error(err)
	}
}

func reportJobLeaseReturned(repository repository.EventStore, job *api.Job, leaseReturnRequest *api.ReturnLeaseRequest) error {
	event, err := api.Wrap(&api.JobLeaseReturnedEvent{
		JobId:        job.Id,
		JobSetId:     job.JobSetId,
		Queue:        job.Queue,
		Created:      time.Now(),
		ClusterId:    leaseReturnRequest.ClusterId,
		Reason:       leaseReturnRequest.Reason,
		KubernetesId: leaseReturnRequest.KubernetesId,
		RunAttempted: leaseReturnRequest.JobRunAttempted,
	})
	if err != nil {
		return fmt.Errorf("error wrapping event: %w", err)
	}

	err = repository.ReportEvents([]*api.EventMessage{event})
	if err != nil {
		return fmt.Errorf("error reporting lease returned event: %w", err)
	}

	return nil
}

func reportJobsCancelling(repository repository.EventStore, requestorName string, jobs []*api.Job) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, err := api.Wrap(&api.JobCancellingEvent{
			JobId:     job.Id,
			Queue:     job.Queue,
			JobSetId:  job.JobSetId,
			Created:   now,
			Requestor: requestorName,
		})
		if err != nil {
			return fmt.Errorf("[reportJobsCancelling] error wrapping event: %w", err)
		}
		events = append(events, event)
	}

	err := repository.ReportEvents(events)
	if err != nil {
		return fmt.Errorf("[reportJobsCancelling] error reporting events: %w", err)
	}

	return nil
}

func reportJobsReprioritizing(repository repository.EventStore, requestorName string, jobs []*api.Job, newPriority float64) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, err := api.Wrap(&api.JobReprioritizingEvent{
			JobId:       job.Id,
			Queue:       job.Queue,
			JobSetId:    job.JobSetId,
			Created:     now,
			NewPriority: newPriority,
			Requestor:   requestorName,
		})
		if err != nil {
			return fmt.Errorf("[reportJobsReprioritizing] error wrapping event: %w", err)
		}
		events = append(events, event)
	}

	err := repository.ReportEvents(events)
	if err != nil {
		return fmt.Errorf("[reportJobsReprioritizing] error reporting events: %w", err)
	}

	return nil
}

func reportJobsReprioritized(repository repository.EventStore, requestorName string, jobs []*api.Job, newPriority float64) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, err := api.Wrap(&api.JobReprioritizedEvent{
			JobId:       job.Id,
			Queue:       job.Queue,
			JobSetId:    job.JobSetId,
			Created:     now,
			NewPriority: newPriority,
			Requestor:   requestorName,
		})
		if err != nil {
			return fmt.Errorf("[reportJobsReprioritized] error wrapping event: %w", err)
		}
		events = append(events, event)
	}

	err := repository.ReportEvents(events)
	if err != nil {
		return fmt.Errorf("[reportJobsReprioritized] error reporting events: %w", err)
	}

	return nil
}

func reportJobsUpdated(repository repository.EventStore, requestorName string, jobs []*api.Job) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, err := api.Wrap(&api.JobUpdatedEvent{
			JobId:     job.Id,
			Queue:     job.Queue,
			JobSetId:  job.JobSetId,
			Created:   now,
			Requestor: requestorName,
			Job:       *job,
		})
		if err != nil {
			return fmt.Errorf("[reportJobsUpdated] error wrapping event: %w", err)
		}
		events = append(events, event)
	}

	err := repository.ReportEvents(events)
	if err != nil {
		return fmt.Errorf("[reportJobsUpdated] error reporting events: %w", err)
	}

	return nil
}

func reportJobsCancelled(repository repository.EventStore, requestorName string, jobs []*api.Job) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, err := api.Wrap(&api.JobCancelledEvent{
			JobId:     job.Id,
			Queue:     job.Queue,
			JobSetId:  job.JobSetId,
			Created:   now,
			Requestor: requestorName,
		})
		if err != nil {
			return fmt.Errorf("[reportJobsCancelled] error wrapping event: %w", err)
		}
		events = append(events, event)
	}

	err := repository.ReportEvents(events)
	if err != nil {
		return fmt.Errorf("[reportJobsCancelled] error reporting events: %w", err)
	}

	return nil
}

type jobFailure struct {
	job    *api.Job
	reason string
}

func reportFailed(repository repository.EventStore, clusterId string, jobFailures []*jobFailure) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, jobFailure := range jobFailures {
		event, err := api.Wrap(&api.JobFailedEvent{
			JobId:        jobFailure.job.Id,
			JobSetId:     jobFailure.job.JobSetId,
			Queue:        jobFailure.job.Queue,
			Created:      now,
			ClusterId:    clusterId,
			Reason:       jobFailure.reason,
			ExitCodes:    make(map[string]int32),
			KubernetesId: "",
			NodeName:     "",
		})
		if err != nil {
			return fmt.Errorf("[reportFailed] error wrapping event: %w", err)
		}
		events = append(events, event)
	}

	err := repository.ReportEvents(events)
	if err != nil {
		return fmt.Errorf("[reportFailed] error reporting events: %w", err)
	}

	return nil
}
