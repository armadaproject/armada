package repository

import (
	"context"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/nats-io/stan.go"
	stanPb "github.com/nats-io/stan.go/pb"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"

	stanUtil "github.com/G-Research/armada/internal/common/stan-util"
	"github.com/G-Research/armada/pkg/api"
)

type NatsEventJobStatusProcessor struct {
	connection    *stanUtil.DurableConnection
	jobRepository JobRepository
	subject       string
	group         string
}

func NewNatsEventJobStatusProcessor(connection *stanUtil.DurableConnection, jobRepository JobRepository, subject string, group string) *NatsEventJobStatusProcessor {
	return &NatsEventJobStatusProcessor{connection: connection, jobRepository: jobRepository, subject: subject, group: group}
}

func (p *NatsEventJobStatusProcessor) Start() {
	err := p.connection.QueueSubscribe(p.subject, p.group,
		p.handleMessage,
		stan.SetManualAckMode(),
		stan.StartAt(stanPb.StartPosition_LastReceived),
		stan.DurableName(p.group))

	if err != nil {
		panic(err)
	}
}

func (p *NatsEventJobStatusProcessor) handleMessage(msg *stan.Msg) {
	// TODO: batching???
	event, err := unmarshal(msg)

	if err != nil {
		log.Errorf("Error while unmarshalling nats message: %v", err)
	} else {
		switch event := event.(type) {
		case *api.JobRunningEvent:
			err = p.jobRepository.UpdateStartTime(event.JobId, event.ClusterId, event.Created)
			if err != nil {
				log.Errorf("Error while updating job start time: %v", err)
				if err.Error() != JobNotFound {
					return
				}
			}
		}
	}
	err = msg.Ack()
	if err != nil {
		log.Errorf("Error while ack nats message: %v", err)
	}
}

func unmarshal(msg *stan.Msg) (api.Event, error) {
	eventMessage := &api.EventMessage{}
	err := proto.Unmarshal(msg.Data, eventMessage)
	if err != nil {
		return nil, err
	}
	return api.UnwrapEvent(eventMessage)
}

type KafkaEventJobStatusProcessor struct {
	reader        *kafka.Reader
	jobRepository JobRepository
}

func NewKafkaJobStatusProcessor(reader *kafka.Reader, jobRepository JobRepository) *KafkaEventJobStatusProcessor {
	return &KafkaEventJobStatusProcessor{reader: reader, jobRepository: jobRepository}
}

func (p *KafkaEventJobStatusProcessor) ProcessEvents() {
	bg := context.Background()

	messages := p.readMessagesBatch(bg, 500*time.Millisecond, 500)
	if len(messages) == 0 {
		return
	}

	events := p.unmarshalEvents(messages)

	for _, apiEvent := range events {
		switch event := apiEvent.(type) {
		case *api.JobRunningEvent:
			err := p.jobRepository.UpdateStartTime(event.JobId, event.ClusterId, event.Created)
			if err != nil {
				log.Errorf("Error while updating job start time: %v", err)
				if err.Error() != JobNotFound {
					return
				}
			}
		}
	}

	err := p.reader.CommitMessages(bg, messages...)
	if err != nil {
		log.Errorf("Unable to commit messages: %v", err)
	}
}

func (p *KafkaEventJobStatusProcessor) readMessagesBatch(ctx context.Context, duration time.Duration, batchSize int) []kafka.Message {
	// Small timeout prevents blocking for long time
	// Kafka Reader buffers messages inside this loops just batches them by 500 or 500ms
	timeout, _ := context.WithDeadline(ctx, time.Now().Add(duration))

	messages := []kafka.Message{}
	for i := 1; i < batchSize; i++ {
		msg, err := p.reader.FetchMessage(timeout)
		if err != nil {
			if err != context.DeadlineExceeded {
				log.Errorf("Error while reading kafka message: %v", err)
			}
			break
		}
		messages = append(messages, msg)
	}
	return messages
}

func (p *KafkaEventJobStatusProcessor) unmarshalEvents(messages []kafka.Message) []api.Event {
	events := []api.Event{}
	for _, msg := range messages {
		eventMessage := &api.EventMessage{}
		err := proto.Unmarshal(msg.Value, eventMessage)
		if err != nil {
			log.Errorf("Error while unmarshaling kafka message: %v", err)
			continue
		}
		event, err := api.UnwrapEvent(eventMessage)
		if err != nil {
			log.Errorf("Error while unmarshaling event message: %v", err)
			continue
		}
		events = append(events, event)
	}
	return events
}
