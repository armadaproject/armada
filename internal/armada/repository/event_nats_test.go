package repository

import (
	"fmt"
	"github.com/G-Research/armada/internal/common/eventstream"
	stan_util "github.com/G-Research/armada/internal/common/stan-util"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestReporting(t *testing.T) {
	durableConnection, err := stan_util.DurableConnect(
		"test-cluster",
		"test-client",
		nats.DefaultURL,
	)
	assert.NoError(t, err)
	store := NewNatsEventStore(durableConnection, "EVENTS")

	nBatches := 1000
	eventsPerBatch := 1000

	start := time.Now()
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

		err := store.ReportEvents(events)
		assert.NoError(t, err)
	}

	elapsed := time.Since(start)
	fmt.Println("Elapsed:", elapsed)
}

func TestReportingStanClient(t *testing.T) {
	nBatches := 1000
	eventsPerBatch := 1000

	durableConnection, err := stan_util.DurableConnect(
		"test-cluster",
		"test-client",
		nats.DefaultURL,
	)
	assert.NoError(t, err)

	stanClient := eventstream.NewStanEventStream(
		"test-cluster",
		"test-client",
		durableConnection,
	)

	start := time.Now()
	for batch := 0; batch < nBatches; batch++ {
		// err := stanClient.Close()
		assert.NoError(t, err)
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

		errs := stanClient.Publish(events)
		assert.Equal(t, 0, len(errs))
	}
	elapsed := time.Since(start)
	fmt.Println("Elapsed:", elapsed)
}

func TestReportingJetstreamClient(t *testing.T) {
	nBatches := 1000
	eventsPerBatch := 1000

	natsConn, err := nats.Connect(nats.DefaultURL)
	assert.NoError(t, err)
	manager, err := jsm.New(natsConn, jsm.WithTimeout(10*time.Second))
	assert.NoError(t, err)
	stream, err := manager.LoadOrNewStream(
		"EVENTS",
		jsm.Subjects("EVENTS"),
		jsm.MaxAge(365*24*time.Hour),
		jsm.FileStorage(),
		jsm.Replicas(1))
	jetstreamClient, err := eventstream.NewJetstreamClient(
		"EVENTS",
		"test-queue",
		natsConn,
		manager,
		stream,
		[]jsm.ConsumerOption{})
	assert.NoError(t, err)

	start := time.Now()
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

		errs := jetstreamClient.Publish(events)
		assert.Equal(t, 0, len(errs))
	}
	elapsed := time.Since(start)
	fmt.Println("Elapsed:", elapsed)
}
