package repository

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/common/eventstream"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

func TestHandleMessage_JobRunningEvent(t *testing.T) {
	acked := false
	runningEventMessage := createJobRunningEventStreamMessage(
		util.NewULID(), "queue", "jobset", "clusterId",
		func() error {
			acked = true
			return nil
		})

	mockBatcher := &mockEventBatcher{}
	processor := NewEventJobStatusProcessor("test", &RedisJobRepository{}, &eventstream.JetstreamEventStream{}, mockBatcher)
	err := processor.handleMessage(runningEventMessage)

	assert.NoError(t, err)
	assert.False(t, acked)
	assert.Len(t, mockBatcher.events, 1)
	assert.Equal(t, mockBatcher.events[0], runningEventMessage)
}

func TestHandleMessage_NonJobRunningEvent(t *testing.T) {
	acked := false
	leasedEventMessage := createJobLeasedEventStreamMessage(
		func() error {
			acked = true
			return nil
		})

	mockBatcher := &mockEventBatcher{}
	processor := NewEventJobStatusProcessor("test", &RedisJobRepository{}, &eventstream.JetstreamEventStream{}, mockBatcher)
	err := processor.handleMessage(leasedEventMessage)

	assert.NoError(t, err)
	assert.True(t, acked)
	assert.Len(t, mockBatcher.events, 0)
}

func createJobLeasedEventStreamMessage(ackFunction eventstream.AckFn) *eventstream.Message {
	eventMessage := &api.EventMessage{
		Events: &api.EventMessage_Leased{
			Leased: &api.JobLeasedEvent{
				JobId:    util.NewULID(),
				JobSetId: "jobset",
				Queue:    "test",
			},
		},
	}
	return &eventstream.Message{
		EventMessage: eventMessage,
		Ack:          ackFunction,
	}
}

func createJobRunningEventStreamMessage(jobId string, queue string, jobSetId string, clusterId string, ackFunction eventstream.AckFn) *eventstream.Message {
	eventMessage := &api.EventMessage{
		Events: &api.EventMessage_Running{
			Running: &api.JobRunningEvent{
				JobId:     jobId,
				JobSetId:  jobSetId,
				Queue:     queue,
				ClusterId: clusterId,
				Created:   time.Now(),
			},
		},
	}

	return &eventstream.Message{
		EventMessage: eventMessage,
		Ack:          ackFunction,
	}
}

type mockEventBatcher struct {
	events []*eventstream.Message
}

func (b *mockEventBatcher) Register(callback eventstream.EventBatchCallback) {
}

func (b *mockEventBatcher) Report(event *eventstream.Message) error {
	b.events = append(b.events, event)
	return nil
}

func (b *mockEventBatcher) Stop() error {
	return nil
}
