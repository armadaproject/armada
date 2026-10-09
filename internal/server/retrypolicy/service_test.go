package retrypolicy

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/internal/common/logging"
	servermocks "github.com/armadaproject/armada/internal/server/mocks"
	"github.com/armadaproject/armada/internal/server/permissions"
	"github.com/armadaproject/armada/internal/server/servertest"
	"github.com/armadaproject/armada/pkg/api"
)

type testMocks struct {
	authorizer *servermocks.MockActionAuthorizer
	repo       *servermocks.MockRetryPolicyRepository
}

func (m *testMocks) expectAuthorizeAction(ctx *armadacontext.Context, perm string, authErr error) {
	m.authorizer.
		EXPECT().
		AuthorizeAction(ctx, permission.Permission(perm)).
		Return(authErr).
		Times(1)
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

// captureLogs returns a context whose logger writes into the returned buffer.
func captureLogs() (*armadacontext.Context, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return armadacontext.New(context.Background(), logging.FromZerolog(zerolog.New(buf))), buf
}

// validPolicy returns a minimal policy that passes ValidatePolicy.
func validPolicy(name string) *api.RetryPolicy {
	return &api.RetryPolicy{
		Name:          name,
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
	}
}

// writeRPCs are the RPCs that check a permission before doing anything else.
var writeRPCs = map[string]struct {
	permission string
	call       func(s *Server, ctx *armadacontext.Context) error
}{
	"create": {
		permission: permissions.CreateRetryPolicy,
		call: func(s *Server, ctx *armadacontext.Context) error {
			_, err := s.CreateRetryPolicy(ctx, validPolicy("p1"))
			return err
		},
	},
	"update": {
		permission: permissions.UpdateRetryPolicy,
		call: func(s *Server, ctx *armadacontext.Context) error {
			_, err := s.UpdateRetryPolicy(ctx, validPolicy("p1"))
			return err
		},
	},
	"delete": {
		permission: permissions.DeleteRetryPolicy,
		call: func(s *Server, ctx *armadacontext.Context) error {
			_, err := s.DeleteRetryPolicy(ctx, &api.RetryPolicyDeleteRequest{Name: "p1"})
			return err
		},
	},
}

// An authorization failure must stop the RPC before it reaches the repository.
// No repository expectation is set, so gomock fails the test if one is called.
func TestRetryPolicyService_AuthorizationFailures(t *testing.T) {
	authOutcomes := map[string]struct {
		authErr  error
		wantCode codes.Code
	}{
		"principal lacks the permission": {
			authErr:  &armadaerrors.ErrUnauthorized{Principal: "alice", Permission: "retry_policy"},
			wantCode: codes.PermissionDenied,
		},
		"authorizer itself fails": {
			authErr:  errors.New("authorizer down"),
			wantCode: codes.Unavailable,
		},
	}
	for rpcName, rpc := range writeRPCs {
		for outcomeName, outcome := range authOutcomes {
			t.Run(rpcName+"/"+outcomeName, func(t *testing.T) {
				s, m := newTestServer(t)
				ctx := armadacontext.Background()
				m.expectAuthorizeAction(ctx, rpc.permission, outcome.authErr)

				servertest.RequireGrpcCode(t, rpc.call(s, ctx), outcome.wantCode)
			})
		}
	}
}

// Every repository error the service distinguishes, and the catch-all that
// makes anything else retryable.
func TestRetryPolicyService_RepositoryErrorMapping(t *testing.T) {
	dbDown := errors.New("postgres down")
	tests := map[string]struct {
		permission string // empty for reads, which are not permission gated
		setupRepo  func(m *testMocks)
		call       func(s *Server, ctx *armadacontext.Context) error
		wantCode   codes.Code
	}{
		"update a missing policy": {
			permission: permissions.UpdateRetryPolicy,
			setupRepo: func(m *testMocks) {
				m.repo.EXPECT().UpdateRetryPolicy(gomock.Any(), gomock.Any()).
					Return(&ErrRetryPolicyNotFound{Name: "p1"}).Times(1)
			},
			call:     writeRPCs["update"].call,
			wantCode: codes.NotFound,
		},
		"get a missing policy": {
			setupRepo: func(m *testMocks) {
				m.repo.EXPECT().GetRetryPolicy(gomock.Any(), "p1").
					Return(nil, &ErrRetryPolicyNotFound{Name: "p1"}).Times(1)
			},
			call: func(s *Server, ctx *armadacontext.Context) error {
				_, err := s.GetRetryPolicy(ctx, &api.RetryPolicyGetRequest{Name: "p1"})
				return err
			},
			wantCode: codes.NotFound,
		},
		"create when the database is down": {
			permission: permissions.CreateRetryPolicy,
			setupRepo: func(m *testMocks) {
				m.repo.EXPECT().CreateRetryPolicy(gomock.Any(), gomock.Any()).Return(dbDown).Times(1)
			},
			call:     writeRPCs["create"].call,
			wantCode: codes.Unavailable,
		},
		"update when the database is down": {
			permission: permissions.UpdateRetryPolicy,
			setupRepo: func(m *testMocks) {
				m.repo.EXPECT().UpdateRetryPolicy(gomock.Any(), gomock.Any()).Return(dbDown).Times(1)
			},
			call:     writeRPCs["update"].call,
			wantCode: codes.Unavailable,
		},
		"delete when the database is down": {
			permission: permissions.DeleteRetryPolicy,
			setupRepo: func(m *testMocks) {
				m.repo.EXPECT().DeleteRetryPolicy(gomock.Any(), "p1").Return(nil, dbDown).Times(1)
			},
			call:     writeRPCs["delete"].call,
			wantCode: codes.Unavailable,
		},
		"get when the database is down": {
			setupRepo: func(m *testMocks) {
				m.repo.EXPECT().GetRetryPolicy(gomock.Any(), "p1").Return(nil, dbDown).Times(1)
			},
			call: func(s *Server, ctx *armadacontext.Context) error {
				_, err := s.GetRetryPolicy(ctx, &api.RetryPolicyGetRequest{Name: "p1"})
				return err
			},
			wantCode: codes.Unavailable,
		},
		"list when the database is down": {
			setupRepo: func(m *testMocks) {
				m.repo.EXPECT().GetAllRetryPolicies(gomock.Any()).Return(nil, dbDown).Times(1)
			},
			call: func(s *Server, ctx *armadacontext.Context) error {
				_, err := s.GetRetryPolicies(ctx, &api.RetryPolicyListRequest{})
				return err
			},
			wantCode: codes.Unavailable,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s, m := newTestServer(t)
			ctx := armadacontext.Background()
			if tc.permission != "" {
				m.expectAuthorizeAction(ctx, tc.permission, nil)
			}
			tc.setupRepo(m)

			servertest.RequireGrpcCode(t, tc.call(s, ctx), tc.wantCode)
		})
	}
}

// Requests the service rejects on its own, before touching the repository. No
// repository expectation is set, so gomock fails the test if one is called.
func TestRetryPolicyService_RejectsBadRequests(t *testing.T) {
	tests := map[string]struct {
		permission string
		call       func(s *Server, ctx *armadacontext.Context) error
	}{
		"create a policy with no default action": {
			permission: permissions.CreateRetryPolicy,
			call: func(s *Server, ctx *armadacontext.Context) error {
				_, err := s.CreateRetryPolicy(ctx, &api.RetryPolicy{Name: "p1"})
				return err
			},
		},
		"update a policy with no default action": {
			permission: permissions.UpdateRetryPolicy,
			call: func(s *Server, ctx *armadacontext.Context) error {
				_, err := s.UpdateRetryPolicy(ctx, &api.RetryPolicy{Name: "p1"})
				return err
			},
		},
		"delete without a name": {
			permission: permissions.DeleteRetryPolicy,
			call: func(s *Server, ctx *armadacontext.Context) error {
				_, err := s.DeleteRetryPolicy(ctx, &api.RetryPolicyDeleteRequest{Name: ""})
				return err
			},
		},
		"get without a name": {
			call: func(s *Server, ctx *armadacontext.Context) error {
				_, err := s.GetRetryPolicy(ctx, &api.RetryPolicyGetRequest{Name: ""})
				return err
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s, m := newTestServer(t)
			ctx := armadacontext.Background()
			if tc.permission != "" {
				m.expectAuthorizeAction(ctx, tc.permission, nil)
			}

			servertest.RequireGrpcCode(t, tc.call(s, ctx), codes.InvalidArgument)
		})
	}
}

// The policy the caller sent must reach the repository unaltered. The
// expectation matches a copy taken beforehand, so a handler that mutated the
// request would no longer satisfy it.
func TestRetryPolicyService_WritesPersistTheRequestedPolicy(t *testing.T) {
	policy := validPolicy("p1")
	policy.RetryLimit = 3
	policy.Rules = []*api.RetryRule{
		{Action: api.RetryAction_RETRY_ACTION_RETRY, OnCategory: "transient"},
	}
	want := proto.Clone(policy).(*api.RetryPolicy)

	tests := map[string]struct {
		permission string
		expectRepo func(m *testMocks)
		call       func(s *Server, ctx *armadacontext.Context) error
	}{
		"create": {
			permission: permissions.CreateRetryPolicy,
			expectRepo: func(m *testMocks) {
				m.repo.EXPECT().CreateRetryPolicy(gomock.Any(), want).Return(nil).Times(1)
			},
			call: func(s *Server, ctx *armadacontext.Context) error {
				_, err := s.CreateRetryPolicy(ctx, policy)
				return err
			},
		},
		"update": {
			permission: permissions.UpdateRetryPolicy,
			expectRepo: func(m *testMocks) {
				m.repo.EXPECT().UpdateRetryPolicy(gomock.Any(), want).Return(nil).Times(1)
			},
			call: func(s *Server, ctx *armadacontext.Context) error {
				_, err := s.UpdateRetryPolicy(ctx, policy)
				return err
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s, m := newTestServer(t)
			ctx := armadacontext.Background()
			m.expectAuthorizeAction(ctx, tc.permission, nil)
			tc.expectRepo(m)

			require.NoError(t, tc.call(s, ctx))
		})
	}
}

func TestRetryPolicyService_ReadsReturnRepositoryContents(t *testing.T) {
	one := &api.RetryPolicy{Name: "p1", RetryLimit: 5}
	all := []*api.RetryPolicy{{Name: "p1", RetryLimit: 3}, {Name: "p2", RetryLimit: 5}}

	tests := map[string]struct {
		expectRepo func(m *testMocks)
		call       func(s *Server, ctx *armadacontext.Context) (any, error)
		want       any
	}{
		"single policy": {
			expectRepo: func(m *testMocks) {
				m.repo.EXPECT().GetRetryPolicy(gomock.Any(), "p1").Return(one, nil).Times(1)
			},
			call: func(s *Server, ctx *armadacontext.Context) (any, error) {
				return s.GetRetryPolicy(ctx, &api.RetryPolicyGetRequest{Name: "p1"})
			},
			want: one,
		},
		"all policies": {
			expectRepo: func(m *testMocks) {
				m.repo.EXPECT().GetAllRetryPolicies(gomock.Any()).Return(all, nil).Times(1)
			},
			call: func(s *Server, ctx *armadacontext.Context) (any, error) {
				res, err := s.GetRetryPolicies(ctx, &api.RetryPolicyListRequest{})
				if err != nil {
					return nil, err
				}
				return res.RetryPolicies, nil
			},
			want: all,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s, m := newTestServer(t)
			ctx := armadacontext.Background()
			tc.expectRepo(m)

			got, err := tc.call(s, ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// Deleting a policy detaches it from queues as a side effect, and the log line
// is the only place that is reported: the RPC returns Empty either way.
func TestDeleteRetryPolicy_LogsDetachedQueues(t *testing.T) {
	tests := map[string]struct {
		detached    []string
		wantLogged  []string
		wantOmitted []string
	}{
		"names every detached queue": {
			detached:   []string{"queue-a", "queue-b"},
			wantLogged: []string{"detached it from 2 queue(s)", "queue-a, queue-b"},
		},
		"stays quiet when nothing referenced the policy": {
			wantOmitted: []string{"detached"},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s, m := newTestServer(t)
			ctx, logs := captureLogs()
			m.expectAuthorizeAction(ctx, permissions.DeleteRetryPolicy, nil)
			m.repo.EXPECT().DeleteRetryPolicy(gomock.Any(), "p1").Return(tc.detached, nil).Times(1)

			_, err := s.DeleteRetryPolicy(ctx, &api.RetryPolicyDeleteRequest{Name: "p1"})
			require.NoError(t, err)
			for _, want := range tc.wantLogged {
				assert.Contains(t, logs.String(), want)
			}
			for _, absent := range tc.wantOmitted {
				assert.NotContains(t, logs.String(), absent)
			}
		})
	}
}
