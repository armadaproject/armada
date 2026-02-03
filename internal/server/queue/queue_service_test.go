package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	commonMocks "github.com/armadaproject/armada/internal/common/mocks"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	servermocks "github.com/armadaproject/armada/internal/server/mocks"
	"github.com/armadaproject/armada/internal/server/permissions"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
	"github.com/armadaproject/armada/pkg/controlplaneevents"
)

type queueServiceTestMocks struct {
	publisher  *commonMocks.MockPublisher[*controlplaneevents.Event]
	authorizer *servermocks.MockActionAuthorizer
	repo       *servermocks.MockQueueRepository
}

func newTestQueueServer(t *testing.T) (*Server, *queueServiceTestMocks) {
	t.Helper()
	ctrl := gomock.NewController(t)
	m := &queueServiceTestMocks{
		publisher:  commonMocks.NewMockPublisher[*controlplaneevents.Event](ctrl),
		authorizer: servermocks.NewMockActionAuthorizer(ctrl),
		repo:       servermocks.NewMockQueueRepository(ctrl),
	}
	s := NewServer(m.publisher, m.repo, m.authorizer)
	return s, m
}

func requireGrpcCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	st, ok := grpcstatus.FromError(err)
	require.True(t, ok, "expected gRPC status error")
	assert.Equal(t, code, st.Code())
}

func TestCreateQueue_PermissionDenied(t *testing.T) {
	s, m := newTestQueueServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CreateQueue)).
		Return(&armadaerrors.ErrUnauthorized{Principal: "alice", Permission: "create_queue"}).
		Times(1)

	_, err := s.CreateQueue(ctx, &api.Queue{Name: "q1"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.PermissionDenied)
}

func TestCreateQueue_AuthorizeErrorUnavailable(t *testing.T) {
	s, m := newTestQueueServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CreateQueue)).
		Return(errors.New("authorizer down")).
		Times(1)

	_, err := s.CreateQueue(ctx, &api.Queue{Name: "q1"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.Unavailable)
}

func TestCreateQueue_DefaultsUserOwnerFromPrincipal(t *testing.T) {
	s, m := newTestQueueServer(t)
	base := armadacontext.Background()
	principal := auth.NewStaticPrincipal("bob", "test", nil)
	grpcCtx := auth.WithPrincipal(base, principal)

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.CreateQueue)).
		Return(nil).
		Times(1)

	var created queue.Queue
	m.repo.
		EXPECT().
		CreateQueue(gomock.Any(), gomock.Any()).
		Do(func(_ *armadacontext.Context, q queue.Queue) {
			created = q
		}).
		Return(nil).
		Times(1)

	_, err := s.CreateQueue(grpcCtx, &api.Queue{Name: "q1", PriorityFactor: 1})
	require.NoError(t, err)

	require.NotEmpty(t, created.Permissions)
	found := false
	for _, perm := range created.Permissions {
		for _, subj := range perm.Subjects {
			if subj.Kind == queue.PermissionSubjectKindUser && subj.Name == "bob" {
				found = true
			}
		}
	}
	assert.True(t, found, "expected default user owner 'bob' to be included in permissions")
}

func TestCreateQueue_ValidationInvalidArgument(t *testing.T) {
	s, m := newTestQueueServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CreateQueue)).
		Return(nil).
		Times(1)

	_, err := s.CreateQueue(ctx, &api.Queue{Name: "q1", PriorityFactor: 1, Labels: map[string]string{"k": ""}})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.InvalidArgument)
}

func TestCreateQueue_AlreadyExists(t *testing.T) {
	s, m := newTestQueueServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CreateQueue)).
		Return(nil).
		Times(1)

	m.repo.
		EXPECT().
		CreateQueue(ctx, gomock.Any()).
		Return(&ErrQueueAlreadyExists{QueueName: "q1"}).
		Times(1)

	_, err := s.CreateQueue(ctx, &api.Queue{Name: "q1", PriorityFactor: 1})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.AlreadyExists)
}

func TestUpdateQueue_NotFound(t *testing.T) {
	s, m := newTestQueueServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CreateQueue)).
		Return(nil).
		Times(1)

	m.repo.
		EXPECT().
		UpdateQueue(ctx, gomock.Any()).
		Return(&ErrQueueNotFound{QueueName: "q1"}).
		Times(1)

	_, err := s.UpdateQueue(ctx, &api.Queue{Name: "q1", PriorityFactor: 1})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.NotFound)
}

func TestDeleteQueue_RepoErrorInvalidArgument(t *testing.T) {
	s, m := newTestQueueServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.DeleteQueue)).
		Return(nil).
		Times(1)

	m.repo.
		EXPECT().
		DeleteQueue(ctx, "q1").
		Return(errors.New("db error")).
		Times(1)

	_, err := s.DeleteQueue(ctx, &api.QueueDeleteRequest{Name: "q1"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.InvalidArgument)
}

func TestGetQueue_NotFound(t *testing.T) {
	s, m := newTestQueueServer(t)
	ctx := armadacontext.Background()

	m.repo.
		EXPECT().
		GetQueue(ctx, "q1").
		Return(queue.Queue{}, &ErrQueueNotFound{QueueName: "q1"}).
		Times(1)

	_, err := s.GetQueue(ctx, &api.QueueGetRequest{Name: "q1"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.NotFound)
}

type fakeQueueStream struct {
	ctx  context.Context
	sent []*api.StreamingQueueMessage
}

func (s *fakeQueueStream) Send(m *api.StreamingQueueMessage) error {
	s.sent = append(s.sent, m)
	return nil
}

func (s *fakeQueueStream) SetHeader(metadata.MD) error  { return nil }
func (s *fakeQueueStream) SendHeader(metadata.MD) error { return nil }
func (s *fakeQueueStream) SetTrailer(metadata.MD)       {}
func (s *fakeQueueStream) Context() context.Context     { return s.ctx }
func (s *fakeQueueStream) SendMsg(interface{}) error    { return nil }
func (s *fakeQueueStream) RecvMsg(interface{}) error    { return nil }

func TestGetQueues_RespectsNumAndSendsEndMarker(t *testing.T) {
	s, m := newTestQueueServer(t)
	ctx := armadacontext.Background()

	queues := []queue.Queue{{Name: "q1", PriorityFactor: 1}, {Name: "q2", PriorityFactor: 1}}
	m.repo.
		EXPECT().
		GetAllQueues(ctx).
		Return(queues, nil).
		Times(1)

	stream := &fakeQueueStream{ctx: ctx}
	err := s.GetQueues(&api.StreamingQueueGetRequest{Num: 1}, stream)
	require.NoError(t, err)
	require.Len(t, stream.sent, 2)

	first := stream.sent[0]
	qMsg, ok := first.Event.(*api.StreamingQueueMessage_Queue)
	require.True(t, ok)
	require.NotNil(t, qMsg.Queue)
	assert.Equal(t, "q1", qMsg.Queue.Name)

	_, ok = stream.sent[1].Event.(*api.StreamingQueueMessage_End)
	assert.True(t, ok, "expected end marker")
}

func TestCordonQueue_Validation(t *testing.T) {
	s, m := newTestQueueServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CordonQueue)).
		Return(nil).
		Times(1)

	_, err := s.CordonQueue(ctx, &api.QueueCordonRequest{Name: ""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot cordon queue with empty name")
}

func TestCancelOnQueue_PublishErrorInternal(t *testing.T) {
	s, m := newTestQueueServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CancelAnyJobs)).
		Return(nil).
		Times(1)

	m.publisher.
		EXPECT().
		PublishMessages(ctx, gomock.Any()).
		Return(errors.New("publish failed")).
		Times(1)

	_, err := s.CancelOnQueue(ctx, &api.QueueCancelRequest{Name: "q1", JobStates: []api.JobState{api.JobState_RUNNING}})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.Internal)
}

func TestCancelOnQueue_SuccessPublishesExpectedEvent(t *testing.T) {
	s, m := newTestQueueServer(t)
	ctx := armadacontext.Background()

	fixedTime := time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)
	s.clock = clocktesting.NewFakeClock(fixedTime)

	req := &api.QueueCancelRequest{
		Name:            "q1",
		PriorityClasses: []string{"pc1", "pc2"},
		JobStates:       []api.JobState{api.JobState_RUNNING, api.JobState_QUEUED},
	}

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CancelAnyJobs)).
		Return(nil).
		Times(1)

	var captured *controlplaneevents.Event
	m.publisher.
		EXPECT().
		PublishMessages(ctx, gomock.Any()).
		Do(func(_ *armadacontext.Context, ev *controlplaneevents.Event) {
			captured = ev
		}).
		Return(nil).
		Times(1)

	_, err := s.CancelOnQueue(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, captured)
	assert.Equal(t, protoutil.ToTimestamp(fixedTime.UTC()), captured.Created)

	wrapped, ok := captured.Event.(*controlplaneevents.Event_CancelOnQueue)
	require.True(t, ok, "expected Event_CancelOnQueue")
	require.NotNil(t, wrapped.CancelOnQueue)
	assert.Equal(t, req.Name, wrapped.CancelOnQueue.Name)
	assert.Equal(t, req.PriorityClasses, wrapped.CancelOnQueue.PriorityClasses)
	assert.Equal(t, []controlplaneevents.ActiveJobState{controlplaneevents.ActiveJobState_RUNNING, controlplaneevents.ActiveJobState_QUEUED}, wrapped.CancelOnQueue.JobStates)
}

func TestPreemptOnQueue_SuccessPublishesExpectedEvent(t *testing.T) {
	s, m := newTestQueueServer(t)
	ctx := armadacontext.Background()

	fixedTime := time.Date(2025, 4, 5, 6, 7, 8, 0, time.UTC)
	s.clock = clocktesting.NewFakeClock(fixedTime)

	req := &api.QueuePreemptRequest{Name: "q1", PriorityClasses: []string{"pc1"}}

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.PreemptAnyJobs)).
		Return(nil).
		Times(1)

	var captured *controlplaneevents.Event
	m.publisher.
		EXPECT().
		PublishMessages(ctx, gomock.Any()).
		Do(func(_ *armadacontext.Context, ev *controlplaneevents.Event) {
			captured = ev
		}).
		Return(nil).
		Times(1)

	_, err := s.PreemptOnQueue(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, captured)
	assert.Equal(t, protoutil.ToTimestamp(fixedTime.UTC()), captured.Created)

	wrapped, ok := captured.Event.(*controlplaneevents.Event_PreemptOnQueue)
	require.True(t, ok, "expected Event_PreemptOnQueue")
	require.NotNil(t, wrapped.PreemptOnQueue)
	assert.Equal(t, req.Name, wrapped.PreemptOnQueue.Name)
	assert.Equal(t, req.PriorityClasses, wrapped.PreemptOnQueue.PriorityClasses)
}

func TestPreemptOnQueue_Validation(t *testing.T) {
	s, m := newTestQueueServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.PreemptAnyJobs)).
		Return(nil).
		Times(1)

	_, err := s.PreemptOnQueue(ctx, &api.QueuePreemptRequest{Name: ""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot preempt jobs on queue with empty name")
}

func TestCancelOnQueue_ValidationJobStates(t *testing.T) {
	s, m := newTestQueueServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CancelAnyJobs)).
		Return(nil).
		Times(1)

	_, err := s.CancelOnQueue(ctx, &api.QueueCancelRequest{Name: "q1", JobStates: []api.JobState{api.JobState_SUCCEEDED}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "provided job states must be non-terminal")
}

func TestCancelOnQueue_ValidationName(t *testing.T) {
	s, m := newTestQueueServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CancelAnyJobs)).
		Return(nil).
		Times(1)

	_, err := s.CancelOnQueue(ctx, &api.QueueCancelRequest{Name: "", JobStates: []api.JobState{api.JobState_RUNNING}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot cancel jobs on queue with empty name")
}

func TestCordonQueue_SuccessCallsRepository(t *testing.T) {
	s, m := newTestQueueServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CordonQueue)).
		Return(nil).
		Times(1)

	m.repo.
		EXPECT().
		CordonQueue(ctx, "q1").
		Return(nil).
		Times(1)

	resp, err := s.CordonQueue(ctx, &api.QueueCordonRequest{Name: "q1"})
	require.NoError(t, err)
	assert.Equal(t, &types.Empty{}, resp)
}
