package repository

import (
	"context"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/pkg/api"
)

type KafkaEventStore struct {
	writer *kafka.Writer
}

func NewKafkaEventStore(writer *kafka.Writer) *KafkaEventStore {
	return &KafkaEventStore{writer: writer}
}

func (k *KafkaEventStore) ReportEvents(messages []*api.EventMessage) error {
	if len(messages) == 0 {
		return nil
	}

	kafkaMessages := []kafka.Message{}

	for _, m := range messages {
		event, e := api.UnwrapEvent(m)
		if e != nil {
			return e
		}
		messageData, e := proto.Marshal(m)
		if e != nil {
			return e
		}
		kafkaMessages = append(kafkaMessages, kafka.Message{
			Key:   []byte(event.GetJobSetId()),
			Value: messageData,
		})
	}

	return k.writer.WriteMessages(context.Background(), kafkaMessages...)
}

type KafkaEventRedisProcessor struct {
	reader     *kafka.Reader
	repository EventStore
}

func NewKafkaEventRedisProcessor(reader *kafka.Reader, repository EventStore) *KafkaEventRedisProcessor {
	return &KafkaEventRedisProcessor{reader: reader, repository: repository}
}

func (p *KafkaEventRedisProcessor) ProcessEvents() {
	bg := context.Background()

	messages := p.readMessagesBatch(bg, 500*time.Millisecond, 500)
	if len(messages) == 0 {
		return
	}

	events := p.unmarshalEvents(messages)

	err := p.repository.ReportEvents(events)
	if err != nil {
		log.Errorf("Unable to save events: %v", err)
		return
	}

	err = p.reader.CommitMessages(bg, messages...)
	if err != nil {
		log.Errorf("Unable to commit messages: %v", err)
		// todo retry???
	}
}

func (p *KafkaEventRedisProcessor) readMessagesBatch(ctx context.Context, duration time.Duration, batchSize int) []kafka.Message {
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

func (p *KafkaEventRedisProcessor) unmarshalEvents(messages []kafka.Message) []*api.EventMessage {
	events := []*api.EventMessage{}
	for _, msg := range messages {
		eventMessage := &api.EventMessage{}
		err := proto.Unmarshal(msg.Value, eventMessage)
		if err != nil {
			log.Errorf("Error while unmarshaling kafka message: %v", err)
			continue
		}
		events = append(events, eventMessage)
	}
	return events
}
