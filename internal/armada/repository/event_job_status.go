package repository

import (
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/eventstream"
	"github.com/G-Research/armada/pkg/api"
)

type EventJobStatusProcessor struct {
	stream        eventstream.EventStream
	queue         string
	jobRepository JobRepository
}

func NewEventJobStatusProcessor(stream eventstream.EventStream, queue string, jobRepository JobRepository) *EventJobStatusProcessor {
	return &EventJobStatusProcessor{stream: stream, queue: queue, jobRepository: jobRepository}
}

func (p *EventJobStatusProcessor) Start() {
	err := p.stream.Subscribe(p.queue, p.handleMessage)

	if err != nil {
		panic(err)
	}
}

func (p *EventJobStatusProcessor) handleMessage(eventMessage *api.EventMessage) error {
	// TODO: batching???
	event, err := api.UnwrapEvent(eventMessage)
	if err != nil {
		log.Errorf("error while unwrapping eventmessage: %v", err)
		return err
	}

	switch event := event.(type) {
	case *api.JobRunningEvent:
		err = p.jobRepository.UpdateStartTime(event.JobId, event.ClusterId, event.Created)
		if err != nil {
			log.Errorf("error while updating job start time: %v", err)
			if err.Error() != JobNotFound {
				return err
			}
		}
	}

	return nil
}
