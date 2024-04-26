package server

import (
	"context"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client/queue"
)

func TestEventServer_Health(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
	defer cancel()
	withEventServer(
		ctx,
		t,
		func(s *EventServer) {
			health, err := s.Health(armadacontext.Background(), &types.Empty{})
			assert.Equal(t, health.Status, api.HealthCheckResponse_SERVING)
			require.NoError(t, err)
		},
	)
}

func TestEventServer_ForceNew(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
	defer cancel()
	withEventServer(
		ctx,
		t,
		func(s *EventServer) {
			jobSetId := "set1"
			queue := ""
			jobIdString := "01f3j0g1md4qx7z5qb148qnh4r"
			runIdString := "123e4567-e89b-12d3-a456-426614174000"
			baseTime, _ := time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
			jobIdProto, _ := armadaevents.ProtoUuidFromUlidString(jobIdString)
			runIdProto := armadaevents.ProtoUuidFromUuid(uuid.MustParse(runIdString))

			stream := &eventStreamMock{}

			assigned := &armadaevents.EventSequence_Event{
				Created: &baseTime,
				Event: &armadaevents.EventSequence_Event_JobRunAssigned{
					JobRunAssigned: &armadaevents.JobRunAssigned{
						RunId: runIdProto,
						JobId: jobIdProto,
					},
				},
			}

			err := reportPulsarEvent(ctx, &armadaevents.EventSequence{
				Queue:      queue,
				JobSetName: jobSetId,
				Events:     []*armadaevents.EventSequence_Event{assigned},
			})

			require.NoError(t, err)
			e := s.GetJobSetEvents(&api.JobSetRequest{Queue: queue, Id: jobSetId, Watch: false, ForceNew: true}, stream)
			assert.NoError(t, e)
			assert.Equal(t, 1, len(stream.sendMessages))
			expected := &api.EventMessage_Pending{Pending: &api.JobPendingEvent{
				JobId:    jobIdString,
				JobSetId: jobSetId,
				Queue:    queue,
				Created:  baseTime,
			}}
			assert.Equal(t, expected, stream.sendMessages[len(stream.sendMessages)-1].Message.Events)
		},
	)
}

func TestEventServer_GetJobSetEvents_EmptyStreamShouldNotFail(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
	defer cancel()
	withEventServer(
		ctx,
		t,
		func(s *EventServer) {
			stream := &eventStreamMock{}
			e := s.GetJobSetEvents(&api.JobSetRequest{Id: "test", Watch: false}, stream)
			require.NoError(t, e)
			assert.Equal(t, 0, len(stream.sendMessages))
		},
	)
}

func TestEventServer_GetJobSetEvents_QueueDoNotExist(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
	defer cancel()
	withEventServer(
		ctx,
		t,
		func(s *EventServer) {
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
		},
	)
}

func TestEventServer_GetJobSetEvents_ErrorIfMissing(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
	defer cancel()
	q := queue.Queue{
		Name:           "test-queue",
		PriorityFactor: 1,
	}

	t.Run("job set non existent ErrorIfMissing true", func(t *testing.T) {
		withEventServer(
			ctx,
			t,
			func(s *EventServer) {
				err := s.queueRepository.CreateQueue(ctx, q)
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
			},
		)
	})

	t.Run("job set non existent ErrorIfMissing false", func(t *testing.T) {
		withEventServer(
			ctx,
			t,
			func(s *EventServer) {
				err := s.queueRepository.CreateQueue(ctx, q)
				assert.NoError(t, err)
				stream := &eventStreamMock{}
				err = s.GetJobSetEvents(&api.JobSetRequest{
					Id:             "job-set-1",
					Watch:          false,
					Queue:          "test-queue",
					ErrorIfMissing: false,
				}, stream)
				assert.NoError(t, err)
			},
		)
	})

	t.Run("job set exists ErrorIfMissing true", func(t *testing.T) {
		withEventServer(
			ctx,
			t,
			func(s *EventServer) {
				err := s.queueRepository.CreateQueue(ctx, q)
				assert.NoError(t, err)
				stream := &eventStreamMock{}

				jobIdString := "01f3j0g1md4qx7z5qb148qnh4r"
				runIdString := "123e4567-e89b-12d3-a456-426614174000"
				baseTime, _ := time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
				jobIdProto, _ := armadaevents.ProtoUuidFromUlidString(jobIdString)
				runIdProto := armadaevents.ProtoUuidFromUuid(uuid.MustParse(runIdString))

				assigned := &armadaevents.EventSequence_Event{
					Created: &baseTime,
					Event: &armadaevents.EventSequence_Event_JobRunAssigned{
						JobRunAssigned: &armadaevents.JobRunAssigned{
							RunId: runIdProto,
							JobId: jobIdProto,
						},
					},
				}

				err = reportPulsarEvent(ctx, &armadaevents.EventSequence{
					Queue:      "test-queue",
					JobSetName: "job-set-1",
					Events:     []*armadaevents.EventSequence_Event{assigned},
				})
				require.NoError(t, err)

				err = s.GetJobSetEvents(&api.JobSetRequest{
					Id:             "job-set-1",
					Watch:          false,
					Queue:          "test-queue",
					ErrorIfMissing: true,
				}, stream)
				require.NoError(t, err)
				assert.Equal(t, 1, len(stream.sendMessages))
			},
		)
	})

	t.Run("job set exists ErrorIfMissing false", func(t *testing.T) {
		withEventServer(
			ctx,
			t,
			func(s *EventServer) {
				err := s.queueRepository.CreateQueue(ctx, q)
				require.NoError(t, err)
				stream := &eventStreamMock{}

				jobIdString := "01f3j0g1md4qx7z5qb148qnh4r"
				runIdString := "123e4567-e89b-12d3-a456-426614174000"
				baseTime, _ := time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
				jobIdProto, _ := armadaevents.ProtoUuidFromUlidString(jobIdString)
				runIdProto := armadaevents.ProtoUuidFromUuid(uuid.MustParse(runIdString))

				assigned := &armadaevents.EventSequence_Event{
					Created: &baseTime,
					Event: &armadaevents.EventSequence_Event_JobRunAssigned{
						JobRunAssigned: &armadaevents.JobRunAssigned{
							RunId: runIdProto,
							JobId: jobIdProto,
						},
					},
				}

				err = reportPulsarEvent(ctx, &armadaevents.EventSequence{
					Queue:      "test-queue",
					JobSetName: "job-set-1",
					Events:     []*armadaevents.EventSequence_Event{assigned},
				})
				require.NoError(t, err)

				err = s.GetJobSetEvents(&api.JobSetRequest{
					Id:             "job-set-1",
					Watch:          false,
					Queue:          "test-queue",
					ErrorIfMissing: false,
				}, stream)
				require.NoError(t, err)
				assert.Equal(t, 1, len(stream.sendMessages))
			},
		)
	})
}

func TestEventServer_GetJobSetEvents_Permissions(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
	defer cancel()
	emptyPerms := make(map[permission.Permission][]string)
	perms := map[permission.Permission][]string{
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
		withEventServer(
			ctx,
			t,
			func(s *EventServer) {
				s.authorizer = NewAuthorizer(authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms))
				err := s.queueRepository.CreateQueue(ctx, q)
				assert.NoError(t, err)

				principal := authorization.NewStaticPrincipal("alice", []string{})
				ctx := authorization.WithPrincipal(armadacontext.Background(), principal)
				stream := &eventStreamMock{ctx: ctx}

				err = s.GetJobSetEvents(&api.JobSetRequest{
					Id:    "job-set-1",
					Watch: false,
					Queue: "test-queue",
				}, stream)
				e, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, codes.PermissionDenied, e.Code())
			},
		)
	})

	t.Run("global permissions", func(t *testing.T) {
		withEventServer(
			ctx,
			t,
			func(s *EventServer) {
				s.authorizer = NewAuthorizer(authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms))
				err := s.queueRepository.CreateQueue(ctx, q)
				assert.NoError(t, err)

				principal := authorization.NewStaticPrincipal("alice", []string{"watch-all-events-group"})
				ctx := authorization.WithPrincipal(armadacontext.Background(), principal)
				stream := &eventStreamMock{ctx: ctx}

				err = s.GetJobSetEvents(&api.JobSetRequest{
					Id:    "job-set-1",
					Watch: false,
					Queue: "test-queue",
				}, stream)
				e, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, codes.OK, e.Code())
			},
		)
	})

	t.Run("queue permission", func(t *testing.T) {
		withEventServer(ctx, t, func(s *EventServer) {
			s.authorizer = NewAuthorizer(authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms))
			err := s.queueRepository.CreateQueue(ctx, q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{"watch-events-group", "watch-queue-group"})
			ctx := authorization.WithPrincipal(armadacontext.Background(), principal)
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

func reportPulsarEvent(ctx *armadacontext.Context, es *armadaevents.EventSequence) error {
	bytes, err := proto.Marshal(es)
	if err != nil {
		return err
	}
	compressor, err := compress.NewZlibCompressor(0)
	if err != nil {
		return err
	}
	compressed, err := compressor.Compress(bytes)
	if err != nil {
		return err
	}

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 11})

	client.XAdd(ctx, &redis.XAddArgs{
		Stream: "Events:" + es.Queue + ":" + es.JobSetName,
		Values: map[string]interface{}{
			"message": compressed,
		},
	})
	return nil
}

func withEventServer(ctx *armadacontext.Context, t *testing.T, action func(s *EventServer)) {
	t.Helper()

	// using real redis instance as miniredis does not support streams
	legacyClient := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 11})

	eventRepo := repository.NewEventRepository(client)
	queueRepo := repository.NewRedisQueueRepository(client)
	jobRepo := repository.NewRedisJobRepository(client)
	server := NewEventServer(&FakeActionAuthorizer{}, eventRepo, queueRepo, jobRepo)

	client.FlushDB(ctx)
	legacyClient.FlushDB(ctx)

	// Create test queue
	err := queueRepo.CreateQueue(ctx, queue.Queue{
		Name:           "",
		Permissions:    nil,
		PriorityFactor: 1,
	})
	require.NoError(t, err)
	action(server)

	client.FlushDB(ctx)
	legacyClient.FlushDB(ctx)
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
		return armadacontext.Background()
	}
	return s.ctx
}
