package server

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/pkg/api"
)

func reportQueued(repository repository.EventStore, jobs []*api.Job) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, e := api.Wrap(&api.JobQueuedEvent{
			JobId:    job.Id,
			Queue:    job.Queue,
			JobSetId: job.JobSetId,
			Created:  now,
		})
		if e != nil {
			return e
		}
		events = append(events, event)
	}
	e := repository.ReportEvents(events)
	return e
}

func reportDuplicateDetected(repository repository.EventStore, results []*repository.SubmitJobResult) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, result := range results {
		event, e := api.Wrap(&api.JobDuplicateFoundEvent{
			JobId:         result.SubmittedJob.Id,
			Queue:         result.SubmittedJob.Queue,
			JobSetId:      result.SubmittedJob.JobSetId,
			Created:       now,
			OriginalJobId: result.JobId,
		})
		if e != nil {
			return e
		}
		events = append(events, event)
	}
	e := repository.ReportEvents(events)
	return e
}

func reportSubmitted(repository repository.EventStore, jobs []*api.Job) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, e := api.Wrap(&api.JobSubmittedEvent{
			JobId:    job.Id,
			Queue:    job.Queue,
			JobSetId: job.JobSetId,
			Created:  now,
			Job:      *job,
		})
		if e != nil {
			return e
		}
		events = append(events, event)
	}

	e := repository.ReportEvents(events)
	return e
}

func reportJobsLeased(repository repository.EventStore, jobs []*api.Job, clusterId string) {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, e := api.Wrap(&api.JobLeasedEvent{
			JobId:     job.Id,
			Queue:     job.Queue,
			JobSetId:  job.JobSetId,
			Created:   now,
			ClusterId: clusterId,
		})
		if e != nil {
			log.Error(e)
		} else {
			events = append(events, event)
		}
	}
	e := repository.ReportEvents(events)
	if e != nil {
		log.Error(e)
	}
}

func reportJobsCancelling(repository repository.EventStore, requestorName string, jobs []*api.Job) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, e := api.Wrap(&api.JobCancellingEvent{
			JobId:     job.Id,
			Queue:     job.Queue,
			JobSetId:  job.JobSetId,
			Created:   now,
			Requestor: requestorName,
		})
		if e != nil {
			return e
		}
		events = append(events, event)
	}
	e := repository.ReportEvents(events)
	return e
}

func reportJobsReprioritizing(repository repository.EventStore, requestorName string, jobs []*api.Job, newPriority float64) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, e := api.Wrap(&api.JobReprioritizingEvent{
			JobId:       job.Id,
			Queue:       job.Queue,
			JobSetId:    job.JobSetId,
			Created:     now,
			NewPriority: newPriority,
			Requestor:   requestorName,
		})
		if e != nil {
			return e
		}
		events = append(events, event)
	}
	e := repository.ReportEvents(events)
	return e
}

func reportJobsReprioritized(repository repository.EventStore, requestorName string, jobs []*api.Job, newPriority float64) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, e := api.Wrap(&api.JobReprioritizedEvent{
			JobId:       job.Id,
			Queue:       job.Queue,
			JobSetId:    job.JobSetId,
			Created:     now,
			NewPriority: newPriority,
			Requestor:   requestorName,
		})
		if e != nil {
			return e
		}
		events = append(events, event)
	}
	e := repository.ReportEvents(events)
	return e
}

func reportJobsUpdated(repository repository.EventStore, requestorName string, jobs []*api.Job) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, e := api.Wrap(&api.JobUpdatedEvent{
			JobId:     job.Id,
			Queue:     job.Queue,
			JobSetId:  job.JobSetId,
			Created:   now,
			Requestor: requestorName,
			Job:       *job,
		})
		if e != nil {
			return e
		}
		events = append(events, event)
	}
	e := repository.ReportEvents(events)
	return e
}

func reportJobsCancelled(repository repository.EventStore, requestorName string, jobs []*api.Job) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, e := api.Wrap(&api.JobCancelledEvent{
			JobId:     job.Id,
			Queue:     job.Queue,
			JobSetId:  job.JobSetId,
			Created:   now,
			Requestor: requestorName,
		})
		if e != nil {
			return e
		}
		events = append(events, event)
	}
	e := repository.ReportEvents(events)
	return e
}

func reportTerminated(repository repository.EventStore, clusterId string, job *api.Job) error {
	event, e := api.Wrap(&api.JobTerminatedEvent{
		JobId:     job.Id,
		Queue:     job.Queue,
		JobSetId:  job.JobSetId,
		Created:   time.Now(),
		ClusterId: clusterId,
	})
	if e != nil {
		return e
	}
	e = repository.ReportEvents([]*api.EventMessage{event})
	return e
}

func reportFailed(repository repository.EventStore, clusterId string, reason string, jobs []*api.Job) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, e := api.Wrap(&api.JobFailedEvent{
			JobId:        job.Id,
			JobSetId:     job.JobSetId,
			Queue:        job.Queue,
			Created:      now,
			ClusterId:    clusterId,
			Reason:       reason,
			ExitCodes:    make(map[string]int32),
			KubernetesId: "",
			NodeName:     "",
		})
		if e != nil {
			return e
		}
		events = append(events, event)
	}
	e := repository.ReportEvents(events)
	return e
}
