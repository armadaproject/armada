package authorization

import (
	"context"

	"github.com/G-Research/armada/internal/armada/authorization/permissions"
)

type Owned interface {
	GetUserOwners() []string
	GetGroupOwners() []string
}

type PermissionChecker interface {
	UserHasPermission(ctx context.Context, perm permissions.Permission) bool
	UserOwns(ctx context.Context, obj Owned) bool
}

type PrincipalPermissionChecker struct {
	permissionGroupMap map[permissions.Permission][]string
	permissionScopeMap map[permissions.Permission][]string
	permissionClaimMap map[permissions.Permission][]string
}

func NewPrincipalPermissionChecker(
	permissionGroupMap map[permissions.Permission][]string,
	permissionScopeMap map[permissions.Permission][]string,
	permissionClaimMap map[permissions.Permission][]string) *PrincipalPermissionChecker {

	return &PrincipalPermissionChecker{
		permissionGroupMap: permissionGroupMap,
		permissionScopeMap: permissionScopeMap,
		permissionClaimMap: permissionClaimMap}
}

func (checker *PrincipalPermissionChecker) UserHasPermission(ctx context.Context, perm permissions.Permission) bool {
	principal := GetPrincipal(ctx)
	return hasPermission(perm, checker.permissionScopeMap, func(scope string) bool { return principal.HasScope(scope) }) ||
		hasPermission(perm, checker.permissionGroupMap, func(group string) bool { return principal.IsInGroup(group) }) ||
		hasPermission(perm, checker.permissionClaimMap, func(claim string) bool { return principal.HasClaim(claim) })
}

func (checker *PrincipalPermissionChecker) UserOwns(ctx context.Context, obj Owned) bool {
	principal := GetPrincipal(ctx)
	currentUserName := principal.GetName()

	for _, userName := range obj.GetUserOwners() {
		if userName == currentUserName {
			return true
		}
	}

	for _, group := range obj.GetGroupOwners() {
		if principal.IsInGroup(group) {
			return true
		}
	}
	return false
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
