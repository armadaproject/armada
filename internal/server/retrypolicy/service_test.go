package retrypolicy

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	servermocks "github.com/armadaproject/armada/internal/server/mocks"
	"github.com/armadaproject/armada/internal/server/permissions"
	"github.com/armadaproject/armada/internal/server/servertest"
	"github.com/armadaproject/armada/pkg/api"
)

type testMocks struct {
	authorizer *servermocks.MockActionAuthorizer
	repo       *servermocks.MockRetryPolicyRepository
}

func newTestServer(t *testing.T) (*Server, *testMocks) {
	t.Helper()
	ctrl := gomock.NewController(t)
	m := &testMocks{
		authorizer: servermocks.NewMockActionAuthorizer(ctrl),
		repo:       servermocks.NewMockRetryPolicyRepository(ctrl),
	}
	s := NewServer(m.repo, m.authorizer)
	return s, m
}

func TestCreateRetryPolicy_PermissionDenied(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CreateRetryPolicy)).
		Return(&armadaerrors.ErrUnauthorized{Principal: "alice", Permission: "create_retry_policy"}).
		Times(1)

	_, err := s.CreateRetryPolicy(ctx, &api.RetryPolicy{Name: "p1"})
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.PermissionDenied)
}

func TestCreateRetryPolicy_AuthorizeErrorUnavailable(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CreateRetryPolicy)).
		Return(errors.New("authorizer down")).
		Times(1)

	_, err := s.CreateRetryPolicy(ctx, &api.RetryPolicy{Name: "p1"})
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.Unavailable)
}

func TestCreateRetryPolicy_EmptyNameInvalidArgument(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CreateRetryPolicy)).
		Return(nil).
		Times(1)

	_, err := s.CreateRetryPolicy(ctx, &api.RetryPolicy{Name: ""})
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.InvalidArgument)
}

func TestCreateRetryPolicy_AlreadyExists(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CreateRetryPolicy)).
		Return(nil).
		Times(1)

	m.repo.
		EXPECT().
		CreateRetryPolicy(gomock.Any(), gomock.Any()).
		Return(&ErrRetryPolicyAlreadyExists{Name: "p1"}).
		Times(1)

	_, err := s.CreateRetryPolicy(ctx, &api.RetryPolicy{Name: "p1"})
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.AlreadyExists)
}

func TestCreateRetryPolicy_Success(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CreateRetryPolicy)).
		Return(nil).
		Times(1)

	m.repo.
		EXPECT().
		CreateRetryPolicy(gomock.Any(), gomock.Any()).
		Return(nil).
		Times(1)

	_, err := s.CreateRetryPolicy(ctx, &api.RetryPolicy{Name: "p1", RetryLimit: 3})
	require.NoError(t, err)
}

func TestUpdateRetryPolicy_EmptyName(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.UpdateRetryPolicy)).
		Return(nil).
		Times(1)

	_, err := s.UpdateRetryPolicy(ctx, &api.RetryPolicy{Name: ""})
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.InvalidArgument)
}

func TestUpdateRetryPolicy_NotFound(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.UpdateRetryPolicy)).
		Return(nil).
		Times(1)

	m.repo.
		EXPECT().
		UpdateRetryPolicy(gomock.Any(), gomock.Any()).
		Return(&ErrRetryPolicyNotFound{Name: "p1"}).
		Times(1)

	_, err := s.UpdateRetryPolicy(ctx, &api.RetryPolicy{Name: "p1"})
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.NotFound)
}

func TestDeleteRetryPolicy_EmptyName(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.DeleteRetryPolicy)).
		Return(nil).
		Times(1)

	_, err := s.DeleteRetryPolicy(ctx, &api.RetryPolicyDeleteRequest{Name: ""})
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.InvalidArgument)
}

func TestDeleteRetryPolicy_PermissionDenied(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.DeleteRetryPolicy)).
		Return(&armadaerrors.ErrUnauthorized{Principal: "alice", Permission: "delete_retry_policy"}).
		Times(1)

	_, err := s.DeleteRetryPolicy(ctx, &api.RetryPolicyDeleteRequest{Name: "p1"})
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.PermissionDenied)
}

func TestDeleteRetryPolicy_Success(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.DeleteRetryPolicy)).
		Return(nil).
		Times(1)

	m.repo.
		EXPECT().
		DeleteRetryPolicy(gomock.Any(), "p1").
		Return(nil).
		Times(1)

	_, err := s.DeleteRetryPolicy(ctx, &api.RetryPolicyDeleteRequest{Name: "p1"})
	require.NoError(t, err)
}

func TestGetRetryPolicy_EmptyName(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := armadacontext.Background()

	_, err := s.GetRetryPolicy(ctx, &api.RetryPolicyGetRequest{Name: ""})
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.InvalidArgument)
}

func TestGetRetryPolicy_NotFound(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.repo.
		EXPECT().
		GetRetryPolicy(gomock.Any(), "p1").
		Return(nil, &ErrRetryPolicyNotFound{Name: "p1"}).
		Times(1)

	_, err := s.GetRetryPolicy(ctx, &api.RetryPolicyGetRequest{Name: "p1"})
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.NotFound)
}

func TestGetRetryPolicy_Success(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	expected := &api.RetryPolicy{Name: "p1", RetryLimit: 5}
	m.repo.
		EXPECT().
		GetRetryPolicy(gomock.Any(), "p1").
		Return(expected, nil).
		Times(1)

	result, err := s.GetRetryPolicy(ctx, &api.RetryPolicyGetRequest{Name: "p1"})
	require.NoError(t, err)
	assert.Equal(t, expected.Name, result.Name)
	assert.Equal(t, expected.RetryLimit, result.RetryLimit)
}

func TestGetRetryPolicies_Success(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	expected := []*api.RetryPolicy{
		{Name: "p1", RetryLimit: 3},
		{Name: "p2", RetryLimit: 5},
	}
	m.repo.
		EXPECT().
		GetAllRetryPolicies(gomock.Any()).
		Return(expected, nil).
		Times(1)

	result, err := s.GetRetryPolicies(ctx, &api.RetryPolicyListRequest{})
	require.NoError(t, err)
	assert.Len(t, result.RetryPolicies, 2)
}
