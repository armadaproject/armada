package authorization

import (
	"context"

	"github.com/armadaproject/armada/internal/common/auth/permission"
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

// UserOwns check if obj is owned by the principal contained in the context,
// or by a group of which the principal is a member.
// If obj is owned by a group of which the principal is a member,
// this method also returns the list of groups that own the object and that the principal is a member of.
// If obj is owned by the principal in the context, no groups are returned.
//
// TODO Should we always return the groups (even if the principal owns obj directly)?
func (checker *PrincipalPermissionChecker) UserOwns(ctx context.Context, obj Owned) (owned bool, ownershipGroups []string) {
	principal := GetPrincipal(ctx)
	currentUserName := principal.GetName()

	for _, userName := range obj.GetUserOwners() {
		if userName == currentUserName {
			return true, []string{}
		}
	}

	ownershipGroups = []string{}
	for _, group := range obj.GetGroupOwners() {
		if principal.IsInGroup(group) {
			ownershipGroups = append(ownershipGroups, group)
		}
	}
	return len(ownershipGroups) > 0, ownershipGroups
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
