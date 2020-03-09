package server

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/pkg/api"
)

func reportQueued(repository repository.EventRepository, jobs []*api.Job) error {
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

func reportSubmitted(repository repository.EventRepository, jobs []*api.Job) error {
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

func reportJobsLeased(repository repository.EventRepository, jobs []*api.Job, clusterId string) {
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

func reportJobsCancelling(repository repository.EventRepository, jobs []*api.Job) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, e := api.Wrap(&api.JobCancellingEvent{
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

func reportJobsCancelled(repository repository.EventRepository, jobs []*api.Job) error {
	events := []*api.EventMessage{}
	now := time.Now()
	for _, job := range jobs {
		event, e := api.Wrap(&api.JobCancelledEvent{
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

func reportTerminated(repository repository.EventRepository, clusterId string, job *api.Job) error {
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
	e = repository.ReportEvent(event)
	return e
}
