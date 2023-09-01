package pulsarutils

import (
	gocontext "context"
	"sync"
	"testing"
	"time"

	ctx "github.com/armadaproject/armada/internal/common/context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/ingest/metrics"
)

var m = metrics.NewMetrics("test_pulsarutils_")

type mockConsumer struct {
	pulsar.Consumer
	msgs     []pulsar.Message
	ackedIds []pulsar.MessageID
}

func (c *mockConsumer) AckID(message pulsar.MessageID) error {
	c.ackedIds = append(c.ackedIds, message)
	return nil
}

func (c *mockConsumer) Receive(ctx gocontext.Context) (pulsar.Message, error) {
	if len(c.msgs) == 0 {
		<-ctx.Done()
		return nil, gocontext.DeadlineExceeded
	}
	msg, newMsgs := c.msgs[0], c.msgs[1:]
	c.msgs = newMsgs
	return msg, nil
}

func TestReceive(t *testing.T) {
	msgTime := time.Now()
	msgs := []pulsar.Message{
		EmptyPulsarMessage(1, msgTime),
		EmptyPulsarMessage(2, msgTime),
		EmptyPulsarMessage(3, msgTime),
	}
	consumer := &mockConsumer{
		msgs: msgs,
	}
	context, cancel := ctx.WithCancel(ctx.Background())
	outputChan := Receive(context, consumer, 10*time.Millisecond, 10*time.Millisecond, m)
	var receivedMsgs []pulsar.Message

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for e := range outputChan {
			receivedMsgs = append(receivedMsgs, e)
			if len(receivedMsgs) == 3 {
				cancel()
				wg.Done()
			}
		}
	}()
	wg.Wait()
	assert.Equal(t, msgs, receivedMsgs)
}

func TestAcks(t *testing.T) {
	input := make(chan []*ConsumerMessageId)
	mockConsumer := mockConsumer{}
	consumers := []pulsar.Consumer{&mockConsumer}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go Ack(ctx.Background(), consumers, input, 1*time.Second, &wg)
	input <- []*ConsumerMessageId{
		{NewMessageId(1), 0, 0}, {NewMessageId(2), 0, 0},
	}
	input <- []*ConsumerMessageId{
		{NewMessageId(3), 0, 0}, {NewMessageId(4), 0, 0},
	}
	close(input)
	expected := []pulsar.MessageID{
		NewMessageId(1),
		NewMessageId(2),
		NewMessageId(3),
		NewMessageId(4),
	}
	wg.Wait()
	assert.Equal(t, expected, mockConsumer.ackedIds)
}
