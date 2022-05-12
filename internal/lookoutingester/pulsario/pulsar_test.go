package pulsario

import (
	ctx "context"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"

	"github.com/G-Research/armada/internal/lookoutingester/model"
	"github.com/G-Research/armada/internal/lookoutingester/testutil"
)

type mockConsumer struct {
	pulsar.Consumer
	msgs     []pulsar.Message
	ackedIds []pulsar.MessageID
}

func (c *mockConsumer) AckID(message pulsar.MessageID) {
	c.ackedIds = append(c.ackedIds, message)
}

func (c *mockConsumer) Receive(ctx context.Context) (pulsar.Message, error) {
	if len(c.msgs) == 0 {
		<-ctx.Done()
		return nil, context.DeadlineExceeded
	}
	msg, newMsgs := c.msgs[0], c.msgs[1:]
	c.msgs = newMsgs
	return msg, nil
}

func TestReceive(t *testing.T) {
	msgTime := time.Now()
	consumer := &mockConsumer{
		msgs: []pulsar.Message{
			testutil.EmptyPulsarMessage(1, msgTime),
			testutil.EmptyPulsarMessage(2, msgTime),
			testutil.EmptyPulsarMessage(3, msgTime),
		},
	}
	context, cancel := ctx.WithCancel(ctx.Background())
	outputChan := Receive(context, consumer, 1, 1, 10*time.Millisecond, 10*time.Millisecond)
	var receivedMsgs []*model.ConsumerMessage

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
	assert.Equal(t, []*model.ConsumerMessage{
		{testutil.EmptyPulsarMessage(1, msgTime), 1},
		{testutil.EmptyPulsarMessage(2, msgTime), 1},
		{testutil.EmptyPulsarMessage(3, msgTime), 1},
	}, receivedMsgs)

}

func TestAcks(t *testing.T) {
	input := make(chan []*model.ConsumerMessageId)
	mockConsumer := mockConsumer{}
	consumers := []pulsar.Consumer{&mockConsumer}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go Ack(ctx.Background(), consumers, input, &wg)
	input <- []*model.ConsumerMessageId{
		{testutil.NewMessageId(1), 0}, {testutil.NewMessageId(2), 0}}
	input <- []*model.ConsumerMessageId{
		{testutil.NewMessageId(3), 0}, {testutil.NewMessageId(4), 0}}
	close(input)
	expected := []pulsar.MessageID{
		testutil.NewMessageId(1),
		testutil.NewMessageId(2),
		testutil.NewMessageId(3),
		testutil.NewMessageId(4),
	}
	wg.Wait()
	assert.Equal(t, expected, mockConsumer.ackedIds)
}
