package repository

import (
	"context"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/gogo/protobuf/proto"
	"github.com/segmentio/kafka-go"
)

type KafkaEventStore struct {
	writer kafka.Writer
}

func (k *KafkaEventStore) ReportEvents(messages []*api.EventMessage) error {
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
