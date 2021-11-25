package repository

import (
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/eventstream"
	"github.com/G-Research/armada/pkg/api"
)

type EventJobStatusProcessor struct {
	queue         string
	jobRepository JobRepository
	stream        eventstream.EventStream
	batcher       eventstream.EventBatcher
}

func NewEventJobStatusProcessor(
	queue string,
	jobRepository JobRepository,
	stream eventstream.EventStream,
	batcher eventstream.EventBatcher,
) *EventJobStatusProcessor {
	batcher.Register(func(events []*api.EventMessage) error {
		var jobStartInfos []*JobStartInfo
		for _, eventMessage := range events {
			event, err := api.UnwrapEvent(eventMessage)
			if err != nil {
				log.Errorf("error while unwrapping event message: %v", err)
				return err
			}
			switch event := event.(type) {
			case *api.JobRunningEvent:
				jobStartInfos = append(jobStartInfos, &JobStartInfo{
					JobId:     event.GetJobId(),
					ClusterId: event.ClusterId,
					StartTime: event.Created,
				})
			}
		}

		jobErrors, err := jobRepository.UpdateStartTime(jobStartInfos)
		if err != nil {
			log.Errorf("error when updating start times for jobs: %v", err)
			return err
		}
		for _, err := range jobErrors {
			if err != nil {
				log.Errorf("error when updating start time for single job: %v", err)
			}
		}
		return nil
	})
	return &EventJobStatusProcessor{
		queue:   queue,
		stream:  stream,
		batcher: batcher,
	}
}

func (p *EventJobStatusProcessor) Start() {
	err := p.stream.Subscribe(p.queue, p.handleMessage)

	if err != nil {
		panic(err)
	}
}

func (p *EventJobStatusProcessor) handleMessage(eventMessage *api.EventMessage) error {
	event, err := api.UnwrapEvent(eventMessage)
	if err != nil {
		log.Errorf("error while unwrapping eventmessage: %v", err)
		return err
	}

	switch event := event.(type) {
	case *api.JobRunningEvent:
		eventMessage, err := api.Wrap(event)
		if err != nil {
			log.Errorf("error when wrapping job running event: %v", err)
		}
		err = p.batcher.Report(eventMessage)
		if err != nil {
			log.Errorf("error when reporting job status event to batcher: %v", err)
		}
	}

	return nil
}
