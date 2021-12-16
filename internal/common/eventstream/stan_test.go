package eventstream

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/nats-io/nats-streaming-server/server"
	"github.com/nats-io/stan.go"
	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

type MockStanClient struct {
	behavior string
}

func (c *MockStanClient) PublishAsync(subject string, data []byte, ah stan.AckHandler) (string, error) {
	if c.behavior == "fail" {
		ah(subject, errors.New("something bad happened"))
		return subject, nil
	}
	if c.behavior == "callback_timeout" {
		// Never invoke the callback
		return subject, nil
	}
	ah(subject, nil)
	return subject, nil
}

func (c *MockStanClient) QueueSubscribe(subject, queue string, cb stan.MsgHandler, opts ...stan.SubscriptionOption) error {
	return nil
}

func (c *MockStanClient) Close() error {
	return nil
}

func TestPublishWithNoError(t *testing.T) {
	stanClient := &MockStanClient{behavior: "success"}
	stream := NewStanEventStream("EVENTS", stanClient)

	nBatches := 1000
	eventsPerBatch := 1000

	for batch := 0; batch < nBatches; batch++ {
		events := make([]*api.EventMessage, eventsPerBatch, eventsPerBatch)
		jobSet := fmt.Sprintf("jobset-%d", batch)
		for i := 0; i < eventsPerBatch; i++ {
			e := &api.EventMessage_Leased{
				Leased: &api.JobLeasedEvent{
					JobId:    util.NewULID(),
					JobSetId: jobSet,
					Queue:    "test",
				},
			}
			event := &api.EventMessage{
				Events: e,
			}
			events[i] = event
		}

		errs := stream.Publish(events)
		assert.Len(t, errs, 0)
	}
}

func TestPublishWithErrors(t *testing.T) {
	stanClient := &MockStanClient{behavior: "fail"}
	stream := NewStanEventStream("EVENTS", stanClient)

	nBatches := 1000
	eventsPerBatch := 1000

	for batch := 0; batch < nBatches; batch++ {
		events := make([]*api.EventMessage, eventsPerBatch, eventsPerBatch)
		jobSet := fmt.Sprintf("jobset-%d", batch)
		for i := 0; i < eventsPerBatch; i++ {
			e := &api.EventMessage_Leased{
				Leased: &api.JobLeasedEvent{
					JobId:    util.NewULID(),
					JobSetId: jobSet,
					Queue:    "test",
				},
			}
			event := &api.EventMessage{
				Events: e,
			}
			events[i] = event
		}

		errs := stream.Publish(events)
		assert.Len(t, errs, eventsPerBatch)
	}
}

func TestPublishWithAckTimeout(t *testing.T) {
	stanClient := &MockStanClient{behavior: "callback_timeout"}
	stream := NewStanEventStream("EVENTS", stanClient)

	nEvents := 1000

	events := make([]*api.EventMessage, nEvents, nEvents)
	jobSet := "jobset-1"
	for i := 0; i < nEvents; i++ {
		e := &api.EventMessage_Leased{
			Leased: &api.JobLeasedEvent{
				JobId:    util.NewULID(),
				JobSetId: jobSet,
				Queue:    "test",
			},
		}
		event := &api.EventMessage{
			Events: e,
		}
		events[i] = event
	}

	errs := stream.Publish(events)
	assert.Len(t, errs, 2) // One timeout for stan, one for the callback
}

func TestStanEvents(t *testing.T) {
	port := 8765

	sOpts := server.GetDefaultOptions()
	sOpts.ID = "test-cluster"
	nOpts := &server.DefaultNatsServerOptions
	nOpts.Port = port
	stanServer, err := server.RunServerWithOpts(sOpts, nOpts)
	assert.NoError(t, err)
	defer stanServer.Shutdown()

	stanClient, err := NewStanClientConnection(
		"test-cluster",
		"test-client",
		[]string{fmt.Sprintf("nats://127.0.0.1:%d", port)},
	)
	assert.NoError(t, err)
	stream := NewStanEventStream(
		"test-cluster",
		stanClient,
		stan.SetManualAckMode(),
		stan.StartWithLastReceived(),
		stan.DeliverAllAvailable(),
	)

	nEvents := 1000

	events := make([]*api.EventMessage, nEvents, nEvents)
	jobSet := "jobset-1"
	for i := 0; i < nEvents; i++ {
		e := &api.EventMessage_Leased{
			Leased: &api.JobLeasedEvent{
				JobId:    util.NewULID(),
				JobSetId: jobSet,
				Queue:    "test",
			},
		}
		event := &api.EventMessage{
			Events: e,
		}
		events[i] = event
	}

	wg := &sync.WaitGroup{}
	wg.Add(nEvents)

	err = stream.Subscribe("test-queue", func(msg *Message) error {
		err := msg.Ack()
		assert.NoError(t, err)
		wg.Done()
		return nil
	})
	assert.NoError(t, err)

	errs := stream.Publish(events)
	assert.Len(t, errs, 0)

	wg.Wait()
}
