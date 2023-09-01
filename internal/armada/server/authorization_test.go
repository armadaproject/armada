package server

import (
	gocontext "context"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
)

type FakePermissionChecker struct{}

func (c FakePermissionChecker) UserOwns(ctx gocontext.Context, obj authorization.Owned) (owned bool, ownershipGroups []string) {
	return true, []string{}
}

func (FakePermissionChecker) UserHasPermission(ctx gocontext.Context, perm permission.Permission) bool {
	return true
}

type FakeDenyAllPermissionChecker struct{}

func (c FakeDenyAllPermissionChecker) UserOwns(ctx gocontext.Context, obj authorization.Owned) (owned bool, ownershipGroups []string) {
	return false, []string{}
}

func (FakeDenyAllPermissionChecker) UserHasPermission(ctx gocontext.Context, perm permission.Permission) bool {
	return false
}
