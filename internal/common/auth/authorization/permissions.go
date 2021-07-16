package authorization

import (
	"context"

	"github.com/G-Research/armada/internal/common/auth/permission"
)

type Owned interface {
	GetUserOwners() []string
	GetGroupOwners() []string
}

type PermissionChecker interface {
	UserHasPermission(ctx context.Context, perm permission.Permission) bool
	UserOwns(ctx context.Context, obj Owned) (owned bool, ownershipGroups []string)
}

type PrincipalPermissionChecker struct {
	permissionGroupMap map[permission.Permission][]string
	permissionScopeMap map[permission.Permission][]string
	permissionClaimMap map[permission.Permission][]string
}

func NewPrincipalPermissionChecker(
	permissionGroupMap map[permission.Permission][]string,
	permissionScopeMap map[permission.Permission][]string,
	permissionClaimMap map[permission.Permission][]string) *PrincipalPermissionChecker {

	return &PrincipalPermissionChecker{
		permissionGroupMap: permissionGroupMap,
		permissionScopeMap: permissionScopeMap,
		permissionClaimMap: permissionClaimMap}
}

func (checker *PrincipalPermissionChecker) UserHasPermission(ctx context.Context, perm permission.Permission) bool {
	principal := GetPrincipal(ctx)
	return hasPermission(perm, checker.permissionScopeMap, func(scope string) bool { return principal.HasScope(scope) }) ||
		hasPermission(perm, checker.permissionGroupMap, func(group string) bool { return principal.IsInGroup(group) }) ||
		hasPermission(perm, checker.permissionClaimMap, func(claim string) bool { return principal.HasClaim(claim) })
}

func (checker *PrincipalPermissionChecker) UserOwns(ctx context.Context, obj Owned) (owned bool, ownershipGoups []string) {
	principal := GetPrincipal(ctx)
	currentUserName := principal.GetName()

	for _, userName := range obj.GetUserOwners() {
		if userName == currentUserName {
			return true, []string{}
		}
	}

	ownershipGoups = []string{}
	for _, group := range obj.GetGroupOwners() {
		if principal.IsInGroup(group) {
			ownershipGoups = append(ownershipGoups, group)
		}
	}
	return len(ownershipGoups) > 0, ownershipGoups
}

func hasPermission(perm permission.Permission, permMap map[permission.Permission][]string, assert func(string) bool) bool {
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
