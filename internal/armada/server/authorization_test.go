package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
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

	authorizedPrincipal := authorization.NewStaticPrincipal("alice", []string{"submit-job-group"})
	unauthorizedPrincipcal := authorization.NewStaticPrincipal("alice", []string{})

	tests := map[string]struct {
		ctx                     *armadacontext.Context
		permissionCheckerResult bool
		expectAuthorized        bool
	}{
		"no permissions": {
			ctx:                     armadacontext.FromGrpcCtx(authorization.WithPrincipal(context.Background(), unauthorizedPrincipcal)),
			permissionCheckerResult: false,
			expectAuthorized:        false,
		},
		"only has global permission": {
			ctx:                     armadacontext.FromGrpcCtx(authorization.WithPrincipal(context.Background(), unauthorizedPrincipcal)),
			permissionCheckerResult: true,
			expectAuthorized:        true,
		},
		"only has queue permission": {
			ctx:                     armadacontext.FromGrpcCtx(authorization.WithPrincipal(context.Background(), authorizedPrincipal)),
			permissionCheckerResult: false,
			expectAuthorized:        true,
		},
		"has both queue and global permissions": {
			ctx:                     armadacontext.FromGrpcCtx(authorization.WithPrincipal(context.Background(), authorizedPrincipal)),
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

type FakeActionAuthorizer struct{}

func (c *FakeActionAuthorizer) AuthorizeAction(ctx *armadacontext.Context, anyPerm permission.Permission) error {
	return nil
}

func (c *FakeActionAuthorizer) AuthorizeQueueAction(
	ctx *armadacontext.Context,
	queue queue.Queue,
	anyPerm permission.Permission,
	perm queue.PermissionVerb,
) error {
	return nil
}

type FakeDenyAllActionAuthorizer struct{}

func (c *FakeDenyAllActionAuthorizer) AuthorizeAction(ctx *armadacontext.Context, anyPerm permission.Permission) error {
	return &armadaerrors.ErrUnauthorized{
		Principal: authorization.GetPrincipal(ctx).GetName(),
		Message:   "permission denied",
	}
}

func (c *FakeDenyAllActionAuthorizer) AuthorizeQueueAction(
	ctx *armadacontext.Context,
	queue queue.Queue,
	anyPerm permission.Permission,
	perm queue.PermissionVerb,
) error {
	return &armadaerrors.ErrUnauthorized{
		Principal: authorization.GetPrincipal(ctx).GetName(),
		Message:   "permission denied",
	}
}

type FakePermissionChecker struct {
	ReturnValue bool
}

func (c FakePermissionChecker) UserOwns(ctx context.Context, obj authorization.Owned) (owned bool, ownershipGroups []string) {
	return c.ReturnValue, []string{}
}

func (c FakePermissionChecker) UserHasPermission(ctx context.Context, perm permission.Permission) bool {
	return c.ReturnValue
}
