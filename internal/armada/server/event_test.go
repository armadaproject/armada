package server

import (
	"context"
	"testing"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/armada/repository"
)

func TestEventServer_ReportUsage(t *testing.T) {
	withEventServer(func(s *EventServer) {

		jobSetId := "set1"
		stream := &eventStreamMock{}

		reportEvent(t, s, &api.JobSubmittedEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobQueuedEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobLeasedEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobLeaseExpiredEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobPendingEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobRunningEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobUnableToScheduleEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobFailedEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobSucceededEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobReprioritizedEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobCancelledEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobTerminatedEvent{JobSetId: jobSetId})

		e := s.GetJobSetEvents(&api.JobSetRequest{Id: jobSetId, Watch: false}, stream)
		assert.Nil(t, e)
		assert.Equal(t, 12, len(stream.sendMessages))

		lastMessage := stream.sendMessages[len(stream.sendMessages)-1]
		reportEvent(t, s, &api.JobCancelledEvent{JobSetId: jobSetId})
		e = s.GetJobSetEvents(&api.JobSetRequest{Id: jobSetId, FromMessageId: lastMessage.Id, Watch: false}, stream)
		assert.Nil(t, e)
		assert.Equal(t, 13, len(stream.sendMessages),
			"Just new messages should be added when reading from last one.")
	})
}

func TestEventServer_GetJobSetEvents_EmptyStreamShouldNotFail(t *testing.T) {
	withEventServer(func(s *EventServer) {

		stream := &eventStreamMock{}
		e := s.GetJobSetEvents(&api.JobSetRequest{Id: "test", Watch: false}, stream)
		assert.Nil(t, e)
		assert.Equal(t, 0, len(stream.sendMessages))
	})
}

func reportEvent(t *testing.T, s *EventServer, event api.Event) {
	msg, _ := api.Wrap(event)
	_, e := s.Report(context.Background(), msg)
	assert.Nil(t, e)
}

func withEventServer(action func(s *EventServer)) {

	// using real redis instance as miniredis does not support streams
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})

	repo := repository.NewRedisEventRepository(client)
	server := NewEventServer(&fakePermissionChecker{}, repo)

	client.FlushDB()

	action(server)

	client.FlushDB()
}

type eventStreamMock struct {
	grpc.ServerStream
	sendMessages []*api.EventStreamMessage
}

func (s *eventStreamMock) Send(m *api.EventStreamMessage) error {
	s.sendMessages = append(s.sendMessages, m)
	return nil
}

func (s *eventStreamMock) Context() context.Context {
	return context.Background()

}
