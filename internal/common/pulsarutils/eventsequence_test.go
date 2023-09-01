package pulsarutils

import (
	gocontext "context"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/schedulers"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestPublishSequences_SendAsyncErr(t *testing.T) {
	producer := &mockProducer{}
	err := PublishSequences(armadacontext.Background(), producer, []*armadaevents.EventSequence{{}}, schedulers.Pulsar)
	assert.NoError(t, err)

	producer = &mockProducer{
		sendAsyncErr: errors.New("sendAsyncErr"),
	}
	err = PublishSequences(armadacontext.Background(), producer, []*armadaevents.EventSequence{{}}, schedulers.Pulsar)
	assert.ErrorIs(t, err, producer.sendAsyncErr)
}

func TestPublishSequences_RespectTimeout(t *testing.T) {
	producer := &mockProducer{
		sendAsyncDuration: 1 * time.Second,
	}
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), time.Millisecond)
	defer cancel()
	err := PublishSequences(ctx, producer, []*armadaevents.EventSequence{{}}, schedulers.Pulsar)
	assert.ErrorIs(t, err, gocontext.DeadlineExceeded)
}

type mockProducer struct {
	sendDuration      time.Duration
	sendAsyncDuration time.Duration
	sendErr           error
	sendAsyncErr      error
	flushErr          error
}

func (producer *mockProducer) Topic() string {
	return "topic"
}

func (producer *mockProducer) Name() string {
	return "name"
}

func (producer *mockProducer) Send(gocontext.Context, *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	time.Sleep(producer.sendDuration)
	return nil, producer.sendErr
}

func (producer *mockProducer) SendAsync(_ gocontext.Context, _ *pulsar.ProducerMessage, f func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	time.Sleep(producer.sendAsyncDuration)
	go f(nil, nil, producer.sendAsyncErr)
}

func (producer *mockProducer) LastSequenceID() int64 {
	return 0
}

func (producer *mockProducer) Flush() error {
	return producer.flushErr
}

func (producer *mockProducer) Close() {}
