package server

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common/context"
	"strings"

	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/pkg/client/queue"
)

// ErrUnauthorized represents an error that occurs when a client tries to perform some action
// through the gRPC API for which it does not have permissions.
// Produces error messages of the form
// "Tom" does not own the queue and have SubmitJobs permissions, "Tom" does not have SubmitAnyJobs permissions
//
// The caller of a function that may produce this error should capture is using errors.As and prepend
// whatever action the principal was attempting.
type ErrUnauthorized struct {
	// Principal that attempted the action
	Principal authorization.Principal
	// Reasons that the principal was not allowed to perform the action
	// For example ["does not own the queue and have SubmitJobs permissions", "does not have SubmitAnyJobs permissions"]
	Reasons []string
}

func (err *ErrUnauthorized) Error() string {
	principalName := err.Principal.GetName()
	reasons := make([]string, len(err.Reasons), len(err.Reasons))
	for i, reason := range err.Reasons {
		reasons[i] = fmt.Sprintf("%q %s", principalName, reason)
	}
	return strings.Join(reasons, ", ")
}

func MergePermissionErrors(errs ...*ErrUnauthorized) *ErrUnauthorized {
	var filtered []*ErrUnauthorized
	for _, err := range errs {
		if err != nil {
			filtered = append(filtered, err)
		}
	}

	if len(filtered) == 0 {
		return nil
	}

	merged := &ErrUnauthorized{
		Principal: filtered[0].Principal,
		Reasons:   []string{},
	}
	for _, err := range filtered {
		merged.Reasons = append(merged.Reasons, err.Reasons...)
	}
	return merged
}

// checkPermission is a helper function called by the gRPC handlers to check if a client has the
// permissions required to perform some action. The error returned is of type ErrUnauthorized.
// After recovering the error (using errors.As), the caller can obtain the name of the user and the
// requested permission programatically via this error type.
func checkPermission(p authorization.PermissionChecker, ctx *context.ArmadaContext, permission permission.Permission) error {
	if !p.UserHasPermission(ctx, permission) {
		return &ErrUnauthorized{
			Principal: authorization.GetPrincipal(ctx),
			Reasons: []string{
				fmt.Sprintf("does not have permission %s", permission),
			},
		}
	}
	return nil
}

func checkQueuePermission(
	p authorization.PermissionChecker,
	ctx *context.ArmadaContext,
	q queue.Queue,
	globalPermission permission.Permission,
	verb queue.PermissionVerb,
) error {
	err := checkPermission(p, ctx, globalPermission)
	if err != nil {
		return err
	}

	// User must either own the queue or have the permission for the specific verb
	owned, _ := p.UserOwns(ctx, q.ToAPI())
	if owned {
		return nil
	}

	principal := authorization.GetPrincipal(ctx)

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
			return nil
		}
	}

	return &ErrUnauthorized{
		Principal: principal,
		Reasons: []string{
			fmt.Sprintf("does not have permission %s", verb),
		},
	}
}
