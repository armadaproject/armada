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
	"github.com/armadaproject/armada/pkg/client/queue"
)

// fakeQueueLister is a stub QueueLister for exercising the delete-time
// referential check without a real queue repository.
type fakeQueueLister struct {
	queues []queue.Queue
	err    error
}

func (f *fakeQueueLister) GetAllQueues(_ *armadacontext.Context) ([]queue.Queue, error) {
	return f.queues, f.err
}

type testMocks struct {
	authorizer  *servermocks.MockActionAuthorizer
	repo        *servermocks.MockRetryPolicyRepository
	queueLister *fakeQueueLister
}

func newTestServer(t *testing.T) (*Server, *testMocks) {
	t.Helper()
	ctrl := gomock.NewController(t)
	m := &testMocks{
		authorizer:  servermocks.NewMockActionAuthorizer(ctrl),
		repo:        servermocks.NewMockRetryPolicyRepository(ctrl),
		queueLister: &fakeQueueLister{},
	}
	s := NewServer(m.repo, m.queueLister, m.authorizer)
	return s, m
}

// validPolicy returns a minimal proto that passes ValidatePolicy, so tests
// targeting downstream behaviour aren't rejected at the upfront validation step.
func validPolicy(name string) *api.RetryPolicy {
	return &api.RetryPolicy{
		Name:          name,
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
	}
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

	_, err := s.CreateRetryPolicy(ctx, validPolicy("p1"))
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.AlreadyExists)
}

func TestCreateRetryPolicy_InvalidPolicyRejected(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CreateRetryPolicy)).
		Return(nil).
		Times(1)

	// DefaultAction defaults to RETRY_ACTION_UNSPECIFIED and no rules are set,
	// which ValidatePolicy rejects. No repo expectation is set: gomock fails
	// if the repository is called.
	_, err := s.CreateRetryPolicy(ctx, &api.RetryPolicy{Name: "p1"})
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.InvalidArgument)
}

func TestCreateRetryPolicy_Success(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.CreateRetryPolicy)).
		Return(nil).
		Times(1)

	var persisted *api.RetryPolicy
	m.repo.
		EXPECT().
		CreateRetryPolicy(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ *armadacontext.Context, p *api.RetryPolicy) error {
			persisted = p
			return nil
		}).
		Times(1)

	policy := validPolicy("p1")
	policy.RetryLimit = 3
	_, err := s.CreateRetryPolicy(ctx, policy)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, "p1", persisted.Name)
	assert.Equal(t, uint32(3), persisted.RetryLimit)
}

func TestUpdateRetryPolicy_PermissionDenied(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.UpdateRetryPolicy)).
		Return(&armadaerrors.ErrUnauthorized{Principal: "alice", Permission: "update_retry_policy"}).
		Times(1)

	_, err := s.UpdateRetryPolicy(ctx, &api.RetryPolicy{Name: "p1"})
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.PermissionDenied)
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

	_, err := s.UpdateRetryPolicy(ctx, validPolicy("p1"))
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.NotFound)
}

func TestUpdateRetryPolicy_InvalidPolicyRejected(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.UpdateRetryPolicy)).
		Return(nil).
		Times(1)

	_, err := s.UpdateRetryPolicy(ctx, &api.RetryPolicy{Name: "p1"})
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.InvalidArgument)
}

func TestUpdateRetryPolicy_Success(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.UpdateRetryPolicy)).
		Return(nil).
		Times(1)

	var persisted *api.RetryPolicy
	m.repo.
		EXPECT().
		UpdateRetryPolicy(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ *armadacontext.Context, p *api.RetryPolicy) error {
			persisted = p
			return nil
		}).
		Times(1)

	policy := validPolicy("p1")
	policy.RetryLimit = 5
	_, err := s.UpdateRetryPolicy(ctx, policy)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, "p1", persisted.Name)
	assert.Equal(t, uint32(5), persisted.RetryLimit)
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

func TestDeleteRetryPolicy_ReferencedByQueues(t *testing.T) {
	tests := map[string]struct {
		queues        []queue.Queue
		wantInMessage []string
		notInMessage  []string
	}{
		"single referencing queue": {
			queues: []queue.Queue{
				{Name: "queue-a", RetryPolicies: []string{"p1"}},
				{Name: "queue-b", RetryPolicies: []string{"other"}},
				{Name: "queue-c"},
			},
			wantInMessage: []string{"queue-a"},
			notInMessage:  []string{"queue-b", "queue-c"},
		},
		"more referencing queues than the reporting cap": {
			queues: []queue.Queue{
				{Name: "q1", RetryPolicies: []string{"p1"}},
				{Name: "q2", RetryPolicies: []string{"p1"}},
				{Name: "q3", RetryPolicies: []string{"p1"}},
				{Name: "q4", RetryPolicies: []string{"p1"}},
				{Name: "q5", RetryPolicies: []string{"p1"}},
				{Name: "q6", RetryPolicies: []string{"p1"}},
			},
			wantInMessage: []string{"6 queue(s)", "q1", "q5"},
			notInMessage:  []string{"q6"},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s, m := newTestServer(t)
			ctx := armadacontext.Background()

			m.authorizer.
				EXPECT().
				AuthorizeAction(ctx, permission.Permission(permissions.DeleteRetryPolicy)).
				Return(nil).
				Times(1)

			// No repo delete expectation: gomock fails the test if the
			// repository is called despite live references.
			m.queueLister.queues = tc.queues

			_, err := s.DeleteRetryPolicy(ctx, &api.RetryPolicyDeleteRequest{Name: "p1"})
			require.Error(t, err)
			servertest.RequireGrpcCode(t, err, codes.FailedPrecondition)
			for _, want := range tc.wantInMessage {
				assert.Contains(t, err.Error(), want)
			}
			for _, notWant := range tc.notInMessage {
				assert.NotContains(t, err.Error(), notWant)
			}
		})
	}
}

func TestDeleteRetryPolicy_QueueListError(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(permissions.DeleteRetryPolicy)).
		Return(nil).
		Times(1)

	m.queueLister.err = errors.New("postgres down")

	_, err := s.DeleteRetryPolicy(ctx, &api.RetryPolicyDeleteRequest{Name: "p1"})
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.Unavailable)
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
	assert.Equal(t, expected, result)
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
	assert.Equal(t, expected, result.RetryPolicies)
}

func TestCreateRetryPolicy_RepoErrorUnavailable(t *testing.T) {
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
		Return(errors.New("db down")).
		Times(1)

	_, err := s.CreateRetryPolicy(ctx, validPolicy("p1"))
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.Unavailable)
}

func TestGetRetryPolicies_RepoErrorUnavailable(t *testing.T) {
	s, m := newTestServer(t)
	ctx := armadacontext.Background()

	m.repo.
		EXPECT().
		GetAllRetryPolicies(gomock.Any()).
		Return(nil, errors.New("db down")).
		Times(1)

	_, err := s.GetRetryPolicies(ctx, &api.RetryPolicyListRequest{})
	require.Error(t, err)
	servertest.RequireGrpcCode(t, err, codes.Unavailable)
}
