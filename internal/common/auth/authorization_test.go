package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/internal/server/permissions"
	"github.com/armadaproject/armada/pkg/client/queue"
)

func TestAuthorizer_AuthorizeAction(t *testing.T) {
	// These tests are very naive - when we rewrite auth a proper set of tests should be considered
	tests := map[string]struct {
		ctx                     *armadacontext.Context
		perm                    permission.Permission
		permissionCheckerResult bool
		expectAuthorized        bool
	}{
		"ContextWithNoPrincipal_Allowed": {
			ctx:                     armadacontext.Background(),
			permissionCheckerResult: true,
			expectAuthorized:        true,
		},
		"ContextWithNoPrincipal_Denied": {
			ctx:                     armadacontext.Background(),
			permissionCheckerResult: false,
			expectAuthorized:        false,
		},
		"ContextWithPrincipal_Allowed": {
			ctx:                     armadacontext.Background(),
			permissionCheckerResult: true,
			expectAuthorized:        true,
		},
		"ContextWithPrincipal_Denied": {
			ctx:                     armadacontext.Background(),
			permissionCheckerResult: false,
			expectAuthorized:        false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			authorizer := NewAuthorizer(&FakePermissionChecker{ReturnValue: tc.permissionCheckerResult})
			result := authorizer.AuthorizeAction(tc.ctx, permissions.CreateQueue)
			if tc.expectAuthorized {
				assert.NoError(t, result)
			} else {
				var permErr *armadaerrors.ErrUnauthorized
				assert.ErrorAs(t, result, &permErr)
			}
		})
	}
}

func TestAuthorizer_AuthorizeQueueAction(t *testing.T) {
	q := queue.Queue{
		Name: "test-queue",
		Permissions: []queue.Permissions{
			{
				Subjects: []queue.PermissionSubject{{
					Kind: "Group",
					Name: "submit-job-group",
				}},
				Verbs: []queue.PermissionVerb{queue.PermissionVerbSubmit},
			},
		},
		PriorityFactor: 1,
	}

	authorizedPrincipal := NewStaticPrincipal("alice", "test", []string{"submit-job-group"})
	unauthorizedPrincipcal := NewStaticPrincipal("alice", "test", []string{})

	tests := map[string]struct {
		ctx                     *armadacontext.Context
		permissionCheckerResult bool
		expectAuthorized        bool
	}{
		"no permissions": {
			ctx:                     armadacontext.FromGrpcCtx(WithPrincipal(context.Background(), unauthorizedPrincipcal)),
			permissionCheckerResult: false,
			expectAuthorized:        false,
		},
		"only has global permission": {
			ctx:                     armadacontext.FromGrpcCtx(WithPrincipal(context.Background(), unauthorizedPrincipcal)),
			permissionCheckerResult: true,
			expectAuthorized:        true,
		},
		"only has queue permission": {
			ctx:                     armadacontext.FromGrpcCtx(WithPrincipal(context.Background(), authorizedPrincipal)),
			permissionCheckerResult: false,
			expectAuthorized:        true,
		},
		"has both queue and global permissions": {
			ctx:                     armadacontext.FromGrpcCtx(WithPrincipal(context.Background(), authorizedPrincipal)),
			permissionCheckerResult: true,
			expectAuthorized:        true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			authorizer := NewAuthorizer(&FakePermissionChecker{ReturnValue: tc.permissionCheckerResult})
			result := authorizer.AuthorizeQueueAction(tc.ctx, q, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit)
			if tc.expectAuthorized {
				assert.NoError(t, result)
			} else {
				var permErr *armadaerrors.ErrUnauthorized
				assert.ErrorAs(t, result, &permErr)
			}
		})
	}
}

type FakePermissionChecker struct {
	ReturnValue bool
}

func (c FakePermissionChecker) UserHasPermission(ctx context.Context, perm permission.Permission) bool {
	return c.ReturnValue
}
