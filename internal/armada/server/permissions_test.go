package server

import (
	"context"

	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/auth/permission"
)

type FakePermissionChecker struct{}

func (c FakePermissionChecker) UserOwns(ctx context.Context, obj authorization.Owned) (owned bool, ownershipGroups []string) {
	return true, []string{}
}

func (FakePermissionChecker) UserHasPermission(ctx context.Context, perm permission.Permission) bool {
	return true
}
