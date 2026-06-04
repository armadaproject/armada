package event

import (
	"context"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/constants"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/server/permissions"
	armadaqueue "github.com/armadaproject/armada/internal/server/queue"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client/queue"
)

type FakeActionAuthorizer struct{}

func (c *FakeActionAuthorizer) AuthorizeAction(_ *armadacontext.Context, _ permission.Permission) error {
	return nil
}

func (c *FakeActionAuthorizer) AuthorizeQueueAction(
	_ *armadacontext.Context,
	_ queue.Queue,
	_ permission.Permission,
	_ queue.PermissionVerb,
) error {
	return nil
}

func TestEventServer_Health(t *testing.T) {
	withEventServer(
		t,
		func(ctx *armadacontext.Context, s *EventServer) {
			health, err := s.Health(armadacontext.Background(), &types.Empty{})
			assert.Equal(t, health.Status, api.HealthCheckResponse_SERVING)
			require.NoError(t, err)
		},
	)
}

func TestEventServer_ForceNew(t *testing.T) {
	withEventServer(
		t,
		func(ctx *armadacontext.Context, s *EventServer) {
			jobSetId := "set1"
			q := queue.Queue{
				Name:           "test-queue",
				PriorityFactor: 1,
			}
			jobId := "01f3j0g1md4qx7z5qb148qnh4r"
			runId := "123e4567-e89b-12d3-a456-426614174000"
			baseTime, _ := time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
			baseTimeProto := protoutil.ToTimestamp(baseTime)

			err := s.queueRepository.(armadaqueue.QueueRepository).CreateQueue(ctx, q)
			require.NoError(t, err)

			stream := &eventStreamMock{}

			assigned := &armadaevents.EventSequence_Event{
				Created: baseTimeProto,
				Event: &armadaevents.EventSequence_Event_JobRunAssigned{
					JobRunAssigned: &armadaevents.JobRunAssigned{
						RunId: runId,
						JobId: jobId,
					},
				},
			}

			err = reportPulsarEvent(ctx, &armadaevents.EventSequence{
				Queue:      q.Name,
				JobSetName: jobSetId,
				Events:     []*armadaevents.EventSequence_Event{assigned},
			})

			require.NoError(t, err)
			e := s.GetJobSetEvents(&api.JobSetRequest{Queue: q.Name, Id: jobSetId, Watch: false}, stream)
			assert.NoError(t, e)
			assert.Equal(t, 1, len(stream.sendMessages))
			expected := &api.EventMessage_Pending{Pending: &api.JobPendingEvent{
				JobId:    jobId,
				JobSetId: jobSetId,
				Queue:    q.Name,
				Created:  protoutil.ToTimestamp(baseTime),
			}}
			assert.Equal(t, expected, stream.sendMessages[len(stream.sendMessages)-1].Message.Events)
		},
	)
}

func TestEventServer_GetJobSetEvents_EmptyStreamShouldNotFail(t *testing.T) {
	withEventServer(
		t,
		func(ctx *armadacontext.Context, s *EventServer) {
			q := queue.Queue{
				Name:           "test-queue",
				PriorityFactor: 1,
			}
			err := s.queueRepository.(armadaqueue.QueueRepository).CreateQueue(ctx, q)
			require.NoError(t, err)
			stream := &eventStreamMock{}
			e := s.GetJobSetEvents(&api.JobSetRequest{Id: "test", Queue: q.Name, Watch: false}, stream)
			require.NoError(t, e)
			assert.Equal(t, 0, len(stream.sendMessages))
		},
	)
}

func TestEventServer_GetJobSetEvents_QueueDoNotExist(t *testing.T) {
	withEventServer(
		t,
		func(_ *armadacontext.Context, s *EventServer) {
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
	q := queue.Queue{
		Name:           "test-queue",
		PriorityFactor: 1,
	}

	tests := map[string]struct {
		publishEvent    bool
		errorIfMissing  bool
		expectErrorCode codes.Code // codes.OK means no error expected
		expectMessages  int
	}{
		"job set non existent, ErrorIfMissing true": {
			errorIfMissing: true, expectErrorCode: codes.NotFound,
		},
		"job set non existent, ErrorIfMissing false": {
			errorIfMissing: false, expectErrorCode: codes.OK,
		},
		"job set exists, ErrorIfMissing true": {
			publishEvent: true, errorIfMissing: true, expectErrorCode: codes.OK, expectMessages: 1,
		},
		"job set exists, ErrorIfMissing false": {
			publishEvent: true, errorIfMissing: false, expectErrorCode: codes.OK, expectMessages: 1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			withEventServer(t, func(ctx *armadacontext.Context, s *EventServer) {
				err := s.queueRepository.(armadaqueue.QueueRepository).CreateQueue(ctx, q)
				require.NoError(t, err)

				if tc.publishEvent {
					require.NoError(t, reportPulsarEvent(ctx, &armadaevents.EventSequence{
						Queue:      "test-queue",
						JobSetName: "job-set-1",
						Events:     []*armadaevents.EventSequence_Event{jobRunAssignedEvent()},
					}))
				}

				stream := &eventStreamMock{}
				err = s.GetJobSetEvents(&api.JobSetRequest{
					Id:             "job-set-1",
					Watch:          false,
					Queue:          "test-queue",
					ErrorIfMissing: tc.errorIfMissing,
				}, stream)

				if tc.expectErrorCode == codes.OK {
					require.NoError(t, err)
				} else {
					e, ok := status.FromError(err)
					assert.True(t, ok)
					assert.Equal(t, tc.expectErrorCode, e.Code())
				}
				assert.Equal(t, tc.expectMessages, len(stream.sendMessages))
			})
		})
	}
}

func TestEventServer_GetJobSetEvents_Permissions(t *testing.T) {
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

	tests := map[string]struct {
		userGroups []string
		expectCode codes.Code
	}{
		"no permissions":     {userGroups: []string{}, expectCode: codes.PermissionDenied},
		"global permissions": {userGroups: []string{"watch-all-events-group"}, expectCode: codes.OK},
		"queue permission":   {userGroups: []string{"watch-events-group", "watch-queue-group"}, expectCode: codes.OK},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			withEventServer(t, func(ctx *armadacontext.Context, s *EventServer) {
				s.authorizer = auth.NewAuthorizer(auth.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms))
				err := s.queueRepository.(armadaqueue.QueueRepository).CreateQueue(ctx, q)
				require.NoError(t, err)

				principal := auth.NewStaticPrincipal("alice", "test", tc.userGroups)
				principalCtx := auth.WithPrincipal(armadacontext.Background(), principal)
				stream := &eventStreamMock{ctx: principalCtx}

				err = s.GetJobSetEvents(&api.JobSetRequest{
					Id:    "job-set-1",
					Watch: false,
					Queue: "test-queue",
				}, stream)
				e, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, tc.expectCode, e.Code())
			})
		})
	}
}

func jobRunAssignedEvent() *armadaevents.EventSequence_Event {
	baseTime, _ := time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	return &armadaevents.EventSequence_Event{
		Created: protoutil.ToTimestamp(baseTime),
		Event: &armadaevents.EventSequence_Event_JobRunAssigned{
			JobRunAssigned: &armadaevents.JobRunAssigned{
				RunId: "123e4567-e89b-12d3-a456-426614174000",
				JobId: "01f3j0g1md4qx7z5qb148qnh4r",
			},
		},
	}
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
		Stream: constants.EventStreamPrefix + es.Queue + ":" + es.JobSetName,
		Values: map[string]interface{}{
			"message": compressed,
		},
	})
	return nil
}

func withEventServer(t *testing.T, action func(ctx *armadacontext.Context, s *EventServer)) {
	t.Helper()
	_ = lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
		defer cancel()

		client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 11})

		eventRepo := NewEventRepository(client)
		queueRepo := armadaqueue.NewPostgresQueueRepository(db)
		server := NewEventServer(&FakeActionAuthorizer{}, eventRepo, queueRepo)
		client.FlushDB(ctx)

		action(ctx, server)

		client.FlushDB(ctx)
		return nil
	})
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
