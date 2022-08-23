package server

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client/queue"
)

func TestEventServer_ReportUsage(t *testing.T) {
	withEventServer(t, configuration.EventRetentionPolicy{ExpiryEnabled: false}, func(s *EventServer) {
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
	withEventServer(t, configuration.EventRetentionPolicy{ExpiryEnabled: false}, func(s *EventServer) {
		stream := &eventStreamMock{}
		e := s.GetJobSetEvents(&api.JobSetRequest{Id: "test", Watch: false}, stream)
		assert.Nil(t, e)
		assert.Equal(t, 0, len(stream.sendMessages))
	})
}

func TestEventServer_GetJobSetEvents_QueueDoNotExist(t *testing.T) {
	withEventServer(t, configuration.EventRetentionPolicy{ExpiryEnabled: false}, func(s *EventServer) {
		stream := &eventStreamMock{}

		err := s.GetJobSetEvents(&api.JobSetRequest{
			Id:             "job-set-1",
			Watch:          false,
			Queue:          "non-existent-queue",
			ErrorIfMissing: false,
		}, stream)
		e, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.NotFound, e.Code())
	})
}

func TestEventServer_GetJobSetEvents_ErrorIfMissing(t *testing.T) {
	q := queue.Queue{
		Name:           "test-queue",
		PriorityFactor: 1,
	}

	t.Run("job set non existent ErrorIfMissing true", func(t *testing.T) {
		withEventServer(t, configuration.EventRetentionPolicy{ExpiryEnabled: false}, func(s *EventServer) {
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)
			stream := &eventStreamMock{}

			err = s.GetJobSetEvents(&api.JobSetRequest{
				Id:             "job-set-1",
				Watch:          false,
				Queue:          "test-queue",
				ErrorIfMissing: true,
			}, stream)
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.NotFound, e.Code())
		})
	})

	t.Run("job set non existent ErrorIfMissing false", func(t *testing.T) {
		withEventServer(t, configuration.EventRetentionPolicy{ExpiryEnabled: false}, func(s *EventServer) {
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)
			stream := &eventStreamMock{}
			err = s.GetJobSetEvents(&api.JobSetRequest{
				Id:             "job-set-1",
				Watch:          false,
				Queue:          "test-queue",
				ErrorIfMissing: false,
			}, stream)
			assert.NoError(t, err)
		})
	})

	t.Run("job set exists ErrorIfMissing true", func(t *testing.T) {
		withEventServer(t, configuration.EventRetentionPolicy{ExpiryEnabled: false}, func(s *EventServer) {
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)
			stream := &eventStreamMock{}

			reportEvent(t, s, &api.JobQueuedEvent{Queue: "test-queue", JobSetId: "job-set-1"})

			err = s.GetJobSetEvents(&api.JobSetRequest{
				Id:             "job-set-1",
				Watch:          false,
				Queue:          "test-queue",
				ErrorIfMissing: true,
			}, stream)
			assert.Nil(t, err)
			assert.Equal(t, 1, len(stream.sendMessages))
		})
	})

	t.Run("job set exists ErrorIfMissing false", func(t *testing.T) {
		withEventServer(t, configuration.EventRetentionPolicy{ExpiryEnabled: false}, func(s *EventServer) {
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)
			stream := &eventStreamMock{}

			reportEvent(t, s, &api.JobQueuedEvent{Queue: "test-queue", JobSetId: "job-set-1"})

			err = s.GetJobSetEvents(&api.JobSetRequest{
				Id:             "job-set-1",
				Watch:          false,
				Queue:          "test-queue",
				ErrorIfMissing: false,
			}, stream)
			assert.Nil(t, err)
			assert.Equal(t, 1, len(stream.sendMessages))
		})
	})
}

func TestEventServer_EventsShouldBeRemovedAfterEventRetentionTime(t *testing.T) {
	eventRetention := configuration.EventRetentionPolicy{ExpiryEnabled: true, RetentionDuration: time.Second * 2}
	withEventServer(t, eventRetention, func(s *EventServer) {
		jobSetId := "set1"
		stream := &eventStreamMock{}
		reportEvent(t, s, &api.JobSubmittedEvent{JobSetId: jobSetId})

		err := s.GetJobSetEvents(&api.JobSetRequest{Id: jobSetId, Watch: false}, stream)
		if ok := assert.NoError(t, err); !ok {
			t.FailNow()
		}
		assert.Equal(t, 1, len(stream.sendMessages))

		time.Sleep(eventRetention.RetentionDuration + time.Millisecond*100)

		stream = &eventStreamMock{}
		err = s.GetJobSetEvents(&api.JobSetRequest{Id: jobSetId, Watch: false}, stream)
		if ok := assert.NoError(t, err); !ok {
			t.FailNow()
		}
		assert.Equal(t, 0, len(stream.sendMessages))
	})
}

func TestEventServer_GetJobSetEvents_Permissions(t *testing.T) {
	emptyPerms := make(map[permission.Permission][]string)
	perms := map[permission.Permission][]string{
		permissions.WatchEvents:    {"watch-events-group"},
		permissions.WatchAllEvents: {"watch-all-events-group"},
	}
	q := queue.Queue{
		Name: "test-queue",
		Permissions: []queue.Permissions{
			{
				Subjects: []queue.PermissionSubject{{
					Kind: "Group",
					Name: "watch-queue-group",
				}},
				Verbs: []queue.PermissionVerb{queue.PermissionVerbWatch},
			},
		},
		PriorityFactor: 1,
	}

	t.Run("no permissions", func(t *testing.T) {
		withEventServer(t, configuration.EventRetentionPolicy{ExpiryEnabled: false}, func(s *EventServer) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{})
			ctx := authorization.WithPrincipal(context.Background(), principal)
			stream := &eventStreamMock{ctx: ctx}

			err = s.GetJobSetEvents(&api.JobSetRequest{
				Id:    "job-set-1",
				Watch: false,
				Queue: "test-queue",
			}, stream)
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("global permissions", func(t *testing.T) {
		withEventServer(t, configuration.EventRetentionPolicy{ExpiryEnabled: false}, func(s *EventServer) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{"watch-all-events-group"})
			ctx := authorization.WithPrincipal(context.Background(), principal)
			stream := &eventStreamMock{ctx: ctx}

			err = s.GetJobSetEvents(&api.JobSetRequest{
				Id:    "job-set-1",
				Watch: false,
				Queue: "test-queue",
			}, stream)
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})

	t.Run("queue permission without specific global permission", func(t *testing.T) {
		withEventServer(t, configuration.EventRetentionPolicy{ExpiryEnabled: false}, func(s *EventServer) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{"watch-queue-group"})
			ctx := authorization.WithPrincipal(context.Background(), principal)
			stream := &eventStreamMock{ctx: ctx}

			err = s.GetJobSetEvents(&api.JobSetRequest{
				Id:    "job-set-1",
				Watch: false,
				Queue: "test-queue",
			}, stream)
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("queue permission", func(t *testing.T) {
		withEventServer(t, configuration.EventRetentionPolicy{ExpiryEnabled: false}, func(s *EventServer) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{"watch-events-group", "watch-queue-group"})
			ctx := authorization.WithPrincipal(context.Background(), principal)
			stream := &eventStreamMock{ctx: ctx}

			err = s.GetJobSetEvents(&api.JobSetRequest{
				Id:    "job-set-1",
				Watch: false,
				Queue: "test-queue",
			}, stream)
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})
}

func reportEvent(t *testing.T, s *EventServer, event api.Event) {
	msg, _ := api.Wrap(event)
	_, e := s.Report(context.Background(), msg)
	assert.Nil(t, e)
}

func withEventServer(t *testing.T, eventRetention configuration.EventRetentionPolicy, action func(s *EventServer)) {
	t.Helper()

	// using real redis instance as miniredis does not support streams
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})

	repo := repository.NewRedisEventRepository(client, eventRetention)
	queueRepo := repository.NewRedisQueueRepository(client)
	server := NewEventServer(&FakePermissionChecker{}, repo, repo, queueRepo, nil)

	client.FlushDB()

	// Create test queue
	err := queueRepo.CreateQueue(queue.Queue{
		Name:           "",
		Permissions:    nil,
		PriorityFactor: 1,
		ResourceLimits: nil,
	})
	assert.NoError(t, err)
	action(server)

	client.FlushDB()
}

type eventStreamMock struct {
	grpc.ServerStream
	ctx          context.Context
	sendMessages []*api.EventStreamMessage
}

func (s *eventStreamMock) Send(m *api.EventStreamMessage) error {
	s.sendMessages = append(s.sendMessages, m)
	return nil
}

func (s *eventStreamMock) Context() context.Context {
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}
