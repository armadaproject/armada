package auth

import (
	"context"

	"github.com/armadaproject/armada/internal/common/auth/permission"
)

type PermissionChecker interface {
	UserHasPermission(ctx context.Context, perm permission.Permission) bool
}

type PrincipalPermissionChecker struct {
	permissionGroupMap map[permission.Permission][]string
	permissionScopeMap map[permission.Permission][]string
	permissionClaimMap map[permission.Permission][]string
}

func NewPrincipalPermissionChecker(
	permissionGroupMap map[permission.Permission][]string,
	permissionScopeMap map[permission.Permission][]string,
	permissionClaimMap map[permission.Permission][]string,
) *PrincipalPermissionChecker {
	return &PrincipalPermissionChecker{
		permissionGroupMap: permissionGroupMap,
		permissionScopeMap: permissionScopeMap,
		permissionClaimMap: permissionClaimMap,
	}
}

// UserHasPermission returns true if the principal contained in the context has the given permission,
// which is determined by checking if any of the groups, scopes, or claims associated with the principal
// has that permission.
func (checker *PrincipalPermissionChecker) UserHasPermission(ctx context.Context, perm permission.Permission) bool {
	principal := GetPrincipal(ctx)
	return hasPermission(perm, checker.permissionScopeMap, principal.HasScope) ||
		hasPermission(perm, checker.permissionGroupMap, principal.IsInGroup) ||
		hasPermission(perm, checker.permissionClaimMap, principal.HasClaim)
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
