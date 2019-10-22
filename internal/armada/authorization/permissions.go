package authorization

import (
	"context"

	"github.com/G-Research/armada/internal/armada/authorization/permissions"
)

type PermissionChecker interface {
	UserHasPermission(ctx context.Context, perm permissions.Permission) bool
}

type PrincipalPermissionChecker struct {
	permissionGroupMap map[permissions.Permission][]string
	permissionScopeMap map[permissions.Permission][]string
}

func NewPrincipalPermissionChecker(
	permissionGroupMap map[permissions.Permission][]string,
	permissionScopeMap map[permissions.Permission][]string) *PrincipalPermissionChecker {

	return &PrincipalPermissionChecker{
		permissionGroupMap: permissionGroupMap,
		permissionScopeMap: permissionScopeMap}
}

func (checker *PrincipalPermissionChecker) UserHasPermission(ctx context.Context, perm permissions.Permission) bool {
	principal := GetPrincipal(ctx)
	return hasPermission(perm, checker.permissionScopeMap, func(scope string) bool { return principal.HasScope(scope) }) ||
		hasPermission(perm, checker.permissionGroupMap, func(group string) bool { return principal.IsInGroup(group) })
}

func hasPermission(perm permissions.Permission, permMap map[permissions.Permission][]string, assert func(string) bool) bool {
	allowedValues, ok := permMap[perm]
	if !ok {
		return false
	}

	for _, value := range allowedValues {
		if assert(value) {
			return true
		}
	}
	return false
}
