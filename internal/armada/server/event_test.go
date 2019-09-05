package server

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
)

func TestEventServer_ReportUsage(t *testing.T) {
	withEventServer(func(s *EventServer) {

		jobSetId := "set1"
		stream := &eventStreamMock{}

		reportEvent(t, s, &api.JobSubmittedEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobQueuedEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobLeasedEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobLeaseExpired{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobPendingEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobRunningEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobFailedEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobSucceededEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobReprioritizedEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobCancellingEvent{JobSetId: jobSetId})
		reportEvent(t, s, &api.JobCancelledEvent{JobSetId: jobSetId})

		e := s.GetJobSetEvents(&api.JobSetRequest{Id: jobSetId, Watch: false}, stream)
		assert.Nil(t, e)
		assert.Equal(t, 11, len(stream.sendMessages))
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
	server := NewEventServer(repo)

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
