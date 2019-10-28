package server

import (
	"context"

	"github.com/G-Research/armada/internal/armada/authorization"
	"github.com/G-Research/armada/internal/armada/authorization/permissions"
)

type fakePermissionChecker struct{}

func (fakePermissionChecker) UserOwns(ctx context.Context, obj authorization.Owned) bool {
	return true
}

func (fakePermissionChecker) UserHasPermission(ctx context.Context, perm permissions.Permission) bool {
	return true
}
