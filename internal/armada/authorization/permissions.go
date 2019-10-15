package authorization

import "context"

type Permission string

const (
	SubmitJobs Permission = "SubmitJobs"
)

type PermissionChecker interface {
	UserHavePermission(ctx context.Context, perm Permission) bool
}

type PrincipalPermissionChecker struct {
	permissionGroupMap map[Permission][]string
}

func NewPrincipalPermissionChecker(permissionGroupMap map[Permission][]string) *PrincipalPermissionChecker {
	return &PrincipalPermissionChecker{permissionGroupMap: permissionGroupMap}
}

func (checker PrincipalPermissionChecker) UserHavePermission(ctx context.Context, perm Permission) bool {
	principal := GetPrincipal(ctx)
	allowedGroups, ok := checker.permissionGroupMap[perm]
	if !ok {
		return true
	}

	for _, group := range allowedGroups {
		if principal.IsInGroup(group) {
			return true
		}
	}
	return false
}
