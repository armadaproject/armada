package server

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	log "github.com/sirupsen/logrus"
	"time"
)

func reportQueued(repository repository.EventRepository, job *api.Job) error {
	event, e := api.Wrap(&api.JobQueuedEvent{
		JobId:    job.Id,
		Queue:    job.Queue,
		JobSetId: job.JobSetId,
		Created:  time.Now(),
	})
	if e != nil {
		return e
	}
	e = repository.ReportEvent(event)
	return e
}

func reportSubmitted(repository repository.EventRepository, job *api.Job) error {
	event, e := api.Wrap(&api.JobSubmittedEvent{
		JobId:    job.Id,
		Queue:    job.Queue,
		JobSetId: job.JobSetId,
		Created:  time.Now(),
		Job:      *job,
	})

	if e != nil {
		return e
	}
	e = repository.ReportEvent(event)
	return e
}

func reportLeased(repository repository.EventRepository, job *api.Job, clusterId string) error {
	event, e := api.Wrap(&api.JobLeasedEvent{
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

func reportJobsLeased(repository repository.EventRepository, jobs []*api.Job, clusterId string) {
	for _, job := range jobs {
		e := reportLeased(repository, job, clusterId)
		if e != nil {
			log.Error(e)
		}
	}
}

func reportJobsCancelling(repository repository.EventRepository, jobs []*api.Job) error {
	events := []*api.EventMessage{}
	for _, job := range jobs {
		event, e := api.Wrap(&api.JobCancellingEvent{
			JobId:    job.Id,
			Queue:    job.Queue,
			JobSetId: job.JobSetId,
			Created:  job.Created,
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
	for _, job := range jobs {
		event, e := api.Wrap(&api.JobCancellingEvent{
			JobId:    job.Id,
			Queue:    job.Queue,
			JobSetId: job.JobSetId,
			Created:  job.Created,
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
		Created:   job.Created,
		ClusterId: clusterId,
	})
	if e != nil {
		return e
	}
	e = repository.ReportEvent(event)
	return e
}
