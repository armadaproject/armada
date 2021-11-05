package repository

import (
	"github.com/G-Research/armada/internal/common/eventstream"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/pkg/api"
)

type NatsEventJobStatusProcessor struct {
	stream        eventstream.EventStream
	jobRepository JobRepository
}

func NewEventJobStatusProcessor(stream eventstream.EventStream, jobRepository JobRepository) *NatsEventJobStatusProcessor {
	return &NatsEventJobStatusProcessor{stream: stream, jobRepository: jobRepository}
}

func (p *NatsEventJobStatusProcessor) Start() {
	err := p.stream.Subscribe(p.handleMessage)

	if err != nil {
		panic(err)
	}
}

func (p *NatsEventJobStatusProcessor) handleMessage(eventMessage *api.EventMessage) error {
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
