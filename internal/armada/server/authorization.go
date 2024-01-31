package server

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/pkg/client/queue"
)

type ActionAuthorizer interface {
	AuthorizeAction(ctx *armadacontext.Context, perm permission.Permission) error
	AuthorizeQueueAction(ctx *armadacontext.Context, queue queue.Queue, anyPerm permission.Permission, perm queue.PermissionVerb) error
}

type Authorizer struct {
	permissionChecker authorization.PermissionChecker
}

func NewAuthorizer(permissionChecker authorization.PermissionChecker) *Authorizer {
	return &Authorizer{
		permissionChecker: permissionChecker,
	}
}

func (b *Authorizer) AuthorizeAction(ctx *armadacontext.Context, perm permission.Permission) error {
	principal := authorization.GetPrincipal(ctx)
	if !b.permissionChecker.UserHasPermission(ctx, perm) {
		return &armadaerrors.ErrUnauthorized{
			Principal:  principal.GetName(),
			Permission: string(perm),
			Action:     string(perm),
			Message:    fmt.Sprintf("user %s does not have permission to perform %s action", principal.GetName(), string(perm)),
		}
	}
	return nil
}

func (b *Authorizer) AuthorizeQueueAction(
	ctx *armadacontext.Context,
	queue queue.Queue,
	anyPerm permission.Permission,
	perm queue.PermissionVerb,
) error {
	principal := authorization.GetPrincipal(ctx)
	hasAnyPerm := b.permissionChecker.UserHasPermission(ctx, anyPerm)
	hasQueuePerm := principalHasQueuePermissions(principal, queue, perm)
	if !hasAnyPerm && !hasQueuePerm {
		return &armadaerrors.ErrUnauthorized{
			Principal:  principal.GetName(),
			Permission: string(perm),
			Action:     string(perm) + " for queue " + queue.Name,
			Message: fmt.Sprintf(
				"user %s cannot perform action %s on queue %s as they are neither explicitly permissioned on the queue "+
					"or a member of %s group", principal.GetName(), string(perm), queue.Name, string(anyPerm)),
		}
	}

	return nil
}

// principalHasQueuePermissions returns true if the principal has permissions to perform some action,
// as specified by the provided verb, for a specific queue, and false otherwise.
func principalHasQueuePermissions(principal authorization.Principal, q queue.Queue, verb queue.PermissionVerb) bool {
	subjects := queue.PermissionSubjects{}
	for _, group := range principal.GetGroupNames() {
		subjects = append(subjects, queue.PermissionSubject{
			Name: group,
			Kind: queue.PermissionSubjectKindGroup,
		})
	}
	subjects = append(subjects, queue.PermissionSubject{
		Name: principal.GetName(),
		Kind: queue.PermissionSubjectKindUser,
	})

	for _, subject := range subjects {
		if q.HasPermission(subject, verb) {
			return true
		}
	}

	return false
}
