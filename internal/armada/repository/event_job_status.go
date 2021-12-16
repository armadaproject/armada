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
	processor := &EventJobStatusProcessor{
		queue:         queue,
		jobRepository: jobRepository,
		stream:        stream,
		batcher:       batcher,
	}
	processor.batcher.Register(processor.handleBatch)
	return processor
}

func (p *EventJobStatusProcessor) Start() {
	err := p.stream.Subscribe(p.queue, p.handleMessage)

	if err != nil {
		panic(err)
	}
}

func (p *EventJobStatusProcessor) handleMessage(message *eventstream.Message) error {
	event, err := api.UnwrapEvent(message.EventMessage)
	if err != nil {
		log.Errorf("error while unwrapping eventmessage: %v", err)
		return err
	}

	switch event.(type) {
	case *api.JobRunningEvent:
		err = p.batcher.Report(message)
		if err != nil {
			log.Errorf("error when reporting job status event to batcher: %v", err)
		}
	}

	return nil
}

func (p *EventJobStatusProcessor) handleBatch(batch []*eventstream.Message) error {
	var jobStartInfos []*JobStartInfo
	for _, msg := range batch {
		event, err := api.UnwrapEvent(msg.EventMessage)
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

	jobErrors, err := p.jobRepository.UpdateStartTime(jobStartInfos)
	if err != nil {
		log.Errorf("error when updating start times for jobs: %v", err)
		return err
	}
	if len(jobErrors) != len(batch) {
		log.Errorf("error when updating start times for jobs: different number of job errors returned")
		return err
	}
	for i, err := range jobErrors {
		if err != nil {
			log.Errorf("error when updating start time for single job: %v", err)
		} else {
			if jobErr := batch[i].Ack(); jobErr != nil {
				log.Errorf("error when acknowledging message: %v", jobErr)
			}
		}
	}
	return nil
}
