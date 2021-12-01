package eventstream

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats-streaming-server/server"
	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

func TestJetstreamEvents(t *testing.T) {
	port := 8369

	opts := &server.DefaultNatsServerOptions
	opts.Port = port
	opts.JetStream = true
	natsServer := test.RunServer(opts)
	defer natsServer.Shutdown()

	jetstreamOpts := &configuration.JetstreamConfig{
		Servers:     []string{fmt.Sprintf("nats://127.0.0.1:%d", port)},
		StreamName:  "EVENTS",
		Replicas:    1,
		Subject:     "EVENTS",
		MaxAgeDays:  1,
		ConnTimeout: 10 * time.Second,
		InMemory:    true,
	}
	eventStream, err := NewJetstreamEventStream(
		jetstreamOpts,
		jsm.SamplePercent(100),
		jsm.StartWithLastReceived())
	assert.NoError(t, err)

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
	wg.Add(1000)

	err = eventStream.Subscribe("test-queue", func(msg *Message) error {
		err := msg.Ack()
		assert.NoError(t, err)
		wg.Done()
		return nil
	})
	assert.NoError(t, err)

	errs := eventStream.Publish(events)
	assert.Len(t, errs, 0)

	wg.Wait()
}
