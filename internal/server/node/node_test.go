package node

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	commonMocks "github.com/armadaproject/armada/internal/common/mocks"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	servermocks "github.com/armadaproject/armada/internal/server/mocks"
	"github.com/armadaproject/armada/internal/server/permissions"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/controlplaneevents"
)

type testMocks struct {
	publisher  *commonMocks.MockPublisher[*controlplaneevents.Event]
	authorizer *servermocks.MockActionAuthorizer
}

func newTestServer(t *testing.T) (*Server, *testMocks) {
	t.Helper()
	ctrl := gomock.NewController(t)
	m := &testMocks{
		publisher:  commonMocks.NewMockPublisher[*controlplaneevents.Event](ctrl),
		authorizer: servermocks.NewMockActionAuthorizer(ctrl),
	}
	s := New(m.publisher, m.authorizer)
	return s, m
}

func requireGrpcCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	st, ok := grpcstatus.FromError(err)
	require.True(t, ok, "expected gRPC status error")
	assert.Equal(t, code, st.Code())
}

func TestPreemptOnNode_PermissionDenied(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.PreemptAnyJobs)).
		Return(&armadaerrors.ErrUnauthorized{Principal: "alice", Permission: "preempt"}).
		Times(1)

	_, err := s.PreemptOnNode(ctx, &api.NodePreemptRequest{Name: "executor-1", Executor: "executor-id"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.PermissionDenied)
}

func TestPreemptOnNode_AuthorizeErrorUnavailable(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.PreemptAnyJobs)).
		Return(errors.New("authorizer down")).
		Times(1)

	_, err := s.PreemptOnNode(ctx, &api.NodePreemptRequest{Name: "executor-1", Executor: "executor-id"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.Internal)
}

func TestPreemptOnNode_Validation(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.PreemptAnyJobs)).
		Return(nil).
		Times(1)

	_, err := s.PreemptOnNode(ctx, &api.NodePreemptRequest{Name: "", Executor: "executor-id"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must provide non-empty executor name")
}

func TestPreemptOnNode_PublishErrorInternal(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.PreemptAnyJobs)).
		Return(nil).
		Times(1)

	m.publisher.
		EXPECT().
		PublishMessages(ctx, gomock.Any()).
		Return(errors.New("publish failed")).
		Times(1)

	_, err := s.PreemptOnNode(ctx, &api.NodePreemptRequest{Name: "executor-1", Executor: "executor-id"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.Internal)
}

func TestPreemptOnNode_SuccessPublishesExpectedEvent(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	fixedTime := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	s.clock = clocktesting.NewFakeClock(fixedTime)

	req := &api.NodePreemptRequest{
		Name:            "executor-1",
		Executor:        "executor-id",
		Queues:          []string{"queue-a", "queue-b"},
		PriorityClasses: []string{"pc1"},
	}

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

	_, err := s.PreemptOnNode(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, captured)
	assert.Equal(t, protoutil.ToTimestamp(fixedTime.UTC()), captured.Created)

	wrapped, ok := captured.Event.(*controlplaneevents.Event_PreemptOnNode)
	require.True(t, ok, "expected Event_PreemptOnNode")
	require.NotNil(t, wrapped.PreemptOnNode)
	assert.Equal(t, req.Name, wrapped.PreemptOnNode.Name)
	assert.Equal(t, req.Executor, wrapped.PreemptOnNode.Executor)
	assert.Equal(t, req.Queues, wrapped.PreemptOnNode.Queues)
	assert.Equal(t, req.PriorityClasses, wrapped.PreemptOnNode.PriorityClasses)
}

func TestCancelOnNode_PermissionDenied(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CancelAnyJobs)).
		Return(&armadaerrors.ErrUnauthorized{Principal: "alice", Permission: "cancel"}).
		Times(1)

	_, err := s.CancelOnNode(ctx, &api.NodeCancelRequest{Name: "executor-1", Executor: "executor-id"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.PermissionDenied)
}

func TestCancelOnNode_AuthorizeErrorUnavailable(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CancelAnyJobs)).
		Return(errors.New("authorizer down")).
		Times(1)

	_, err := s.CancelOnNode(ctx, &api.NodeCancelRequest{Name: "executor-1", Executor: "executor-id"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.Internal)
}

func TestCancelOnNode_Validation(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CancelAnyJobs)).
		Return(nil).
		Times(1)

	_, err := s.CancelOnNode(ctx, &api.NodeCancelRequest{Name: "", Executor: "executor-id"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must provide non-empty executor name")
}

func TestCancelOnNode_PublishErrorInternal(t *testing.T) {
	s, m := newTestServer(t)
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

	_, err := s.CancelOnNode(ctx, &api.NodeCancelRequest{Name: "executor-1", Executor: "executor-id"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.Internal)
}

func TestCancelOnNode_SuccessPublishesExpectedEvent(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	fixedTime := time.Date(2025, 2, 3, 4, 5, 6, 0, time.UTC)
	s.clock = clocktesting.NewFakeClock(fixedTime)

	req := &api.NodeCancelRequest{
		Name:            "executor-1",
		Executor:        "executor-id",
		Queues:          []string{"queue-a"},
		PriorityClasses: []string{"pc1", "pc2"},
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

	_, err := s.CancelOnNode(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, captured)
	assert.Equal(t, protoutil.ToTimestamp(fixedTime.UTC()), captured.Created)

	wrapped, ok := captured.Event.(*controlplaneevents.Event_CancelOnNode)
	require.True(t, ok, "expected Event_CancelOnNode")
	require.NotNil(t, wrapped.CancelOnNode)
	assert.Equal(t, req.Name, wrapped.CancelOnNode.Name)
	assert.Equal(t, req.Executor, wrapped.CancelOnNode.Executor)
	assert.Equal(t, req.Queues, wrapped.CancelOnNode.Queues)
	assert.Equal(t, req.PriorityClasses, wrapped.CancelOnNode.PriorityClasses)
}
