package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func XTest_KafkaBasic(t *testing.T) {
	brokers := []string{"localhost:9092"}

	go func() {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{"localhost:9092"},
			Topic:    "ArmadaEvents",
			GroupID:  "ArmadaEventRedisProcessor",
			MaxWait:  500 * time.Millisecond,
			MinBytes: 0,    // 10KB
			MaxBytes: 10e6, // 10MB
		})
		//r.SetOffset(0)

		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				break
			}
			fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
		}

		r.Close()

	}()

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   "ArmadaEvents",
		//Balancer: &kafka.LeastBytes{},
	})
	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		//kafka.Message{
		//	Key:   []byte("Key-B"),
		//	Value: []byte("One!"),
		//},
		//kafka.Message{
		//	Key:   []byte("Key-C"),
		//	Value: []byte("Two!"),
		//},
	)
	assert.NoError(t, err)

	w.Close()

}
