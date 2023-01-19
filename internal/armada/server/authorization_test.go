package server

import (
	"context"

	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
)

type FakePermissionChecker struct{}

func (c FakePermissionChecker) UserOwns(ctx context.Context, obj authorization.Owned) (owned bool, ownershipGroups []string) {
	return true, []string{}
}

func (FakePermissionChecker) UserHasPermission(ctx context.Context, perm permission.Permission) bool {
	return true
}

type FakeDenyAllPermissionChecker struct{}

func (c FakeDenyAllPermissionChecker) UserOwns(ctx context.Context, obj authorization.Owned) (owned bool, ownershipGroups []string) {
	return false, []string{}
}

func (FakeDenyAllPermissionChecker) UserHasPermission(ctx context.Context, perm permission.Permission) bool {
	return false
}
