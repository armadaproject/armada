package processor

import (
	"errors"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/eventstream"
	"github.com/G-Research/armada/pkg/api"
)

type EventJobStatusProcessor struct {
	queue         string
	jobRepository repository.JobRepository
	stream        eventstream.EventStream
	batcher       eventstream.EventBatcher
}

func NewEventJobStatusProcessor(
	queue string,
	jobRepository repository.JobRepository,
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
	default:
		err := message.Ack()
		if err != nil {
			log.Errorf("error when acknowledging message: %v", err)
		}
	}

	return nil
}

func (p *EventJobStatusProcessor) handleBatch(batch []*eventstream.Message) error {
	var jobStartInfos []*repository.JobStartInfo
	var runningEventMessages []*eventstream.Message
	for _, msg := range batch {
		event, err := api.UnwrapEvent(msg.EventMessage)
		if err != nil {
			log.Errorf("error while unwrapping event message: %v", err)
			return err
		}
		switch event := event.(type) {
		case *api.JobRunningEvent:
			jobStartInfos = append(jobStartInfos, &repository.JobStartInfo{
				JobId:     event.GetJobId(),
				ClusterId: event.ClusterId,
				StartTime: event.Created,
			})
			runningEventMessages = append(runningEventMessages, msg)
		default:
			err := msg.Ack()
			if err != nil {
				log.Errorf("error when acknowledging message: %v", err)
			}
		}
	}

	jobErrors, err := p.jobRepository.UpdateStartTime(jobStartInfos)
	if err != nil {
		log.Errorf("error when updating start times for jobs: %v", err)
		return err
	}
	if len(jobErrors) != len(jobStartInfos) {
		log.Errorf("error when updating start times for jobs: different number of job errors returned")
		return err
	}
	var jobNotFoundError *repository.ErrJobNotFound
	for i, err := range jobErrors {
		// Ack JobNotFound, as we don't need to record start time for jobs that no longer exist
		if err != nil && !errors.As(err, &jobNotFoundError) {
			log.Errorf("error when updating start time for single job: %v", err)
		} else {
			if jobErr := runningEventMessages[i].Ack(); jobErr != nil {
				log.Errorf("error when acknowledging message: %v", jobErr)
			}
		}
	}
	return nil
}
