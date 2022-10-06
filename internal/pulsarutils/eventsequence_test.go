package pulsarutils

import (
	"context"
	"testing"

	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestPublishSequences_Error(t *testing.T) {
	producer := &mockProducer{}
	err := PublishSequences(context.Background(), producer, []*armadaevents.EventSequence{
		{},
	})
	assert.NoError(t, err)

	producer = &mockProducer{
		sendAsyncErr: errors.New("sendAsyncErr"),
	}
	err = PublishSequences(context.Background(), producer, []*armadaevents.EventSequence{{}})
	assert.ErrorIs(t, err, producer.sendAsyncErr)
}

type mockProducer struct {
	sendErr      error
	sendAsyncErr error
}

func (producer *mockProducer) Topic() string {
	return "topic"
}

func (producer *mockProducer) Name() string {
	return "name"
}

func (producer *mockProducer) Send(context.Context, *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	return nil, producer.sendErr
}

func (producer *mockProducer) SendAsync(_ context.Context, _ *pulsar.ProducerMessage, f func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	go f(nil, nil, producer.sendAsyncErr)
}

func (producer *mockProducer) LastSequenceID() int64 {
	return 0
}

func (producer *mockProducer) Flush() error {
	return nil
}

func (producer *mockProducer) Close() {}
