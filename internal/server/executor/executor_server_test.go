package executor

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
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	commonMocks "github.com/armadaproject/armada/internal/common/mocks"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	servermocks "github.com/armadaproject/armada/internal/server/mocks"
	"github.com/armadaproject/armada/internal/server/permissions"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/controlplaneevents"
)

type executorTestMocks struct {
	publisher  *commonMocks.MockPublisher[*controlplaneevents.Event]
	authorizer *servermocks.MockActionAuthorizer
}

func newExecutorTestServer(t *testing.T) (*Server, *executorTestMocks) {
	t.Helper()
	ctrl := gomock.NewController(t)
	m := &executorTestMocks{
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

func TestUpsertExecutorSettings_PermissionDenied(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.UpdateExecutorSettings)).
		Return(&armadaerrors.ErrUnauthorized{Principal: "alice", Permission: "update_executor_settings"}).
		Times(1)

	_, err := s.UpsertExecutorSettings(grpcCtx, &api.ExecutorSettingsUpsertRequest{Name: "executor-1"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.PermissionDenied)
}

func TestUpsertExecutorSettings_AuthorizeErrorUnavailable(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.UpdateExecutorSettings)).
		Return(errors.New("authorizer down")).
		Times(1)

	_, err := s.UpsertExecutorSettings(grpcCtx, &api.ExecutorSettingsUpsertRequest{Name: "executor-1"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.Unavailable)
}

func TestUpsertExecutorSettings_ValidationName(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.UpdateExecutorSettings)).
		Return(nil).
		Times(1)

	_, err := s.UpsertExecutorSettings(grpcCtx, &api.ExecutorSettingsUpsertRequest{Name: ""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must provide non-empty executor name")
}

func TestUpsertExecutorSettings_ValidationCordonReason(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.UpdateExecutorSettings)).
		Return(nil).
		Times(1)

	_, err := s.UpsertExecutorSettings(grpcCtx, &api.ExecutorSettingsUpsertRequest{Name: "executor-1", Cordoned: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cordon reason must be specified if cordoning")
}

func TestUpsertExecutorSettings_PublishErrorInternal(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.UpdateExecutorSettings)).
		Return(nil).
		Times(1)

	m.publisher.
		EXPECT().
		PublishMessages(gomock.Any(), gomock.Any()).
		Return(errors.New("publish failed")).
		Times(1)

	_, err := s.UpsertExecutorSettings(grpcCtx, &api.ExecutorSettingsUpsertRequest{Name: "executor-1"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.Internal)
}

func TestUpsertExecutorSettings_SuccessPublishesExpectedEvent(t *testing.T) {
	s, m := newExecutorTestServer(t)

	fixedTime := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	s.clock = clocktesting.NewFakeClock(fixedTime)

	principal := auth.NewStaticPrincipal("alice", "test", []string{"group-a"})
	grpcCtx := auth.WithPrincipal(armadacontext.Background(), principal)

	req := &api.ExecutorSettingsUpsertRequest{Name: "executor-1", Cordoned: true, CordonReason: "maintenance"}

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.UpdateExecutorSettings)).
		Return(nil).
		Times(1)

	var capturedCtx *armadacontext.Context
	var captured *controlplaneevents.Event
	m.publisher.
		EXPECT().
		PublishMessages(gomock.Any(), gomock.Any()).
		Do(func(ctx *armadacontext.Context, ev *controlplaneevents.Event) {
			capturedCtx = ctx
			captured = ev
		}).
		Return(nil).
		Times(1)

	_, err := s.UpsertExecutorSettings(grpcCtx, req)
	require.NoError(t, err)
	require.NotNil(t, captured)
	require.NotNil(t, capturedCtx)

	assert.Equal(t, "alice", auth.GetPrincipal(capturedCtx).GetName())
	assert.Equal(t, protoutil.ToTimestamp(fixedTime.UTC()), captured.Created)

	wrapped, ok := captured.Event.(*controlplaneevents.Event_ExecutorSettingsUpsert)
	require.True(t, ok, "expected Event_ExecutorSettingsUpsert")
	require.NotNil(t, wrapped.ExecutorSettingsUpsert)
	assert.Equal(t, req.Name, wrapped.ExecutorSettingsUpsert.Name)
	assert.Equal(t, req.Cordoned, wrapped.ExecutorSettingsUpsert.Cordoned)
	assert.Equal(t, req.CordonReason, wrapped.ExecutorSettingsUpsert.CordonReason)
	assert.Equal(t, "alice", wrapped.ExecutorSettingsUpsert.SetByUser)
}

func TestDeleteExecutorSettings_PermissionDenied(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.UpdateExecutorSettings)).
		Return(&armadaerrors.ErrUnauthorized{Principal: "alice", Permission: "update_executor_settings"}).
		Times(1)

	_, err := s.DeleteExecutorSettings(grpcCtx, &api.ExecutorSettingsDeleteRequest{Name: "executor-1"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.PermissionDenied)
}

func TestDeleteExecutorSettings_AuthorizeErrorUnavailable(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.UpdateExecutorSettings)).
		Return(errors.New("authorizer down")).
		Times(1)

	_, err := s.DeleteExecutorSettings(grpcCtx, &api.ExecutorSettingsDeleteRequest{Name: "executor-1"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.Unavailable)
}

func TestDeleteExecutorSettings_ValidationName(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.UpdateExecutorSettings)).
		Return(nil).
		Times(1)

	_, err := s.DeleteExecutorSettings(grpcCtx, &api.ExecutorSettingsDeleteRequest{Name: ""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must provide non-empty executor name")
}

func TestDeleteExecutorSettings_PublishErrorInternal(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.UpdateExecutorSettings)).
		Return(nil).
		Times(1)

	m.publisher.
		EXPECT().
		PublishMessages(gomock.Any(), gomock.Any()).
		Return(errors.New("publish failed")).
		Times(1)

	_, err := s.DeleteExecutorSettings(grpcCtx, &api.ExecutorSettingsDeleteRequest{Name: "executor-1"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.Internal)
}

func TestDeleteExecutorSettings_SuccessPublishesExpectedEvent(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	fixedTime := time.Date(2025, 2, 3, 4, 5, 6, 0, time.UTC)
	s.clock = clocktesting.NewFakeClock(fixedTime)

	req := &api.ExecutorSettingsDeleteRequest{Name: "executor-1"}

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.UpdateExecutorSettings)).
		Return(nil).
		Times(1)

	var captured *controlplaneevents.Event
	m.publisher.
		EXPECT().
		PublishMessages(gomock.Any(), gomock.Any()).
		Do(func(_ *armadacontext.Context, ev *controlplaneevents.Event) {
			captured = ev
		}).
		Return(nil).
		Times(1)

	_, err := s.DeleteExecutorSettings(grpcCtx, req)
	require.NoError(t, err)
	require.NotNil(t, captured)
	assert.Equal(t, protoutil.ToTimestamp(fixedTime.UTC()), captured.Created)

	wrapped, ok := captured.Event.(*controlplaneevents.Event_ExecutorSettingsDelete)
	require.True(t, ok, "expected Event_ExecutorSettingsDelete")
	require.NotNil(t, wrapped.ExecutorSettingsDelete)
	assert.Equal(t, req.Name, wrapped.ExecutorSettingsDelete.Name)
}

func TestPreemptOnExecutor_PermissionDenied(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.PreemptAnyJobs)).
		Return(&armadaerrors.ErrUnauthorized{Principal: "alice", Permission: "preempt_any_jobs"}).
		Times(1)

	_, err := s.PreemptOnExecutor(grpcCtx, &api.ExecutorPreemptRequest{Name: "executor-1"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.PermissionDenied)
}

func TestPreemptOnExecutor_AuthorizeErrorUnavailable(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.PreemptAnyJobs)).
		Return(errors.New("authorizer down")).
		Times(1)

	_, err := s.PreemptOnExecutor(grpcCtx, &api.ExecutorPreemptRequest{Name: "executor-1"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.Unavailable)
}

func TestPreemptOnExecutor_ValidationName(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.PreemptAnyJobs)).
		Return(nil).
		Times(1)

	_, err := s.PreemptOnExecutor(grpcCtx, &api.ExecutorPreemptRequest{Name: ""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must provide non-empty executor name")
}

func TestPreemptOnExecutor_PublishErrorInternal(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.PreemptAnyJobs)).
		Return(nil).
		Times(1)

	m.publisher.
		EXPECT().
		PublishMessages(gomock.Any(), gomock.Any()).
		Return(errors.New("publish failed")).
		Times(1)

	_, err := s.PreemptOnExecutor(grpcCtx, &api.ExecutorPreemptRequest{Name: "executor-1"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.Internal)
}

func TestPreemptOnExecutor_SuccessPublishesExpectedEvent(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	fixedTime := time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)
	s.clock = clocktesting.NewFakeClock(fixedTime)

	req := &api.ExecutorPreemptRequest{Name: "executor-1", Queues: []string{"queue-a"}, PriorityClasses: []string{"pc1", "pc2"}}

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.PreemptAnyJobs)).
		Return(nil).
		Times(1)

	var captured *controlplaneevents.Event
	m.publisher.
		EXPECT().
		PublishMessages(gomock.Any(), gomock.Any()).
		Do(func(_ *armadacontext.Context, ev *controlplaneevents.Event) {
			captured = ev
		}).
		Return(nil).
		Times(1)

	_, err := s.PreemptOnExecutor(grpcCtx, req)
	require.NoError(t, err)
	require.NotNil(t, captured)
	assert.Equal(t, protoutil.ToTimestamp(fixedTime.UTC()), captured.Created)

	wrapped, ok := captured.Event.(*controlplaneevents.Event_PreemptOnExecutor)
	require.True(t, ok, "expected Event_PreemptOnExecutor")
	require.NotNil(t, wrapped.PreemptOnExecutor)
	assert.Equal(t, req.Name, wrapped.PreemptOnExecutor.Name)
	assert.Equal(t, req.Queues, wrapped.PreemptOnExecutor.Queues)
	assert.Equal(t, req.PriorityClasses, wrapped.PreemptOnExecutor.PriorityClasses)
}

func TestCancelOnExecutor_PermissionDenied(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.CancelAnyJobs)).
		Return(&armadaerrors.ErrUnauthorized{Principal: "alice", Permission: "cancel_any_jobs"}).
		Times(1)

	_, err := s.CancelOnExecutor(grpcCtx, &api.ExecutorCancelRequest{Name: "executor-1"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.PermissionDenied)
}

func TestCancelOnExecutor_AuthorizeErrorUnavailable(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.CancelAnyJobs)).
		Return(errors.New("authorizer down")).
		Times(1)

	_, err := s.CancelOnExecutor(grpcCtx, &api.ExecutorCancelRequest{Name: "executor-1"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.Unavailable)
}

func TestCancelOnExecutor_ValidationName(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.CancelAnyJobs)).
		Return(nil).
		Times(1)

	_, err := s.CancelOnExecutor(grpcCtx, &api.ExecutorCancelRequest{Name: ""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must provide non-empty executor name")
}

func TestCancelOnExecutor_PublishErrorInternal(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.CancelAnyJobs)).
		Return(nil).
		Times(1)

	m.publisher.
		EXPECT().
		PublishMessages(gomock.Any(), gomock.Any()).
		Return(errors.New("publish failed")).
		Times(1)

	_, err := s.CancelOnExecutor(grpcCtx, &api.ExecutorCancelRequest{Name: "executor-1"})
	require.Error(t, err)
	requireGrpcCode(t, err, codes.Internal)
}

func TestCancelOnExecutor_SuccessPublishesExpectedEvent(t *testing.T) {
	s, m := newExecutorTestServer(t)
	grpcCtx := armadacontext.Background()

	fixedTime := time.Date(2025, 4, 5, 6, 7, 8, 0, time.UTC)
	s.clock = clocktesting.NewFakeClock(fixedTime)

	req := &api.ExecutorCancelRequest{Name: "executor-1", Queues: []string{"queue-a", "queue-b"}, PriorityClasses: []string{"pc1"}}

	m.authorizer.
		EXPECT().
		AuthorizeAction(gomock.Any(), permission.Permission(permissions.CancelAnyJobs)).
		Return(nil).
		Times(1)

	var captured *controlplaneevents.Event
	m.publisher.
		EXPECT().
		PublishMessages(gomock.Any(), gomock.Any()).
		Do(func(_ *armadacontext.Context, ev *controlplaneevents.Event) {
			captured = ev
		}).
		Return(nil).
		Times(1)

	_, err := s.CancelOnExecutor(grpcCtx, req)
	require.NoError(t, err)
	require.NotNil(t, captured)
	assert.Equal(t, protoutil.ToTimestamp(fixedTime.UTC()), captured.Created)

	wrapped, ok := captured.Event.(*controlplaneevents.Event_CancelOnExecutor)
	require.True(t, ok, "expected Event_CancelOnExecutor")
	require.NotNil(t, wrapped.CancelOnExecutor)
	assert.Equal(t, req.Name, wrapped.CancelOnExecutor.Name)
	assert.Equal(t, req.Queues, wrapped.CancelOnExecutor.Queues)
	assert.Equal(t, req.PriorityClasses, wrapped.CancelOnExecutor.PriorityClasses)
}
