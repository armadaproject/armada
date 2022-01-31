package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/pkg/client/queue"
)

// ErrNoPermission represents an error that occurs when a client tries to perform some action
// through the gRPC API for which it does not have permissions.
// Produces error messages of the form
// "Tom" does not own the queue and have SubmitJobs permissions, "Tom" does not have SubmitAnyJobs permissions
//
// The caller of a function that may produce this error should capture is using errors.As and prepend
// whatever action the principal was attempting.
type ErrNoPermission struct {
	// Principal that attempted the action
	Principal authorization.Principal
	// Reasons that the principal was not allowed to perform the action
	// For example ["does not own the queue and have SubmitJobs permissions", "does not have SubmitAnyJobs permissions"]
	Reasons []string
}

func (err *ErrNoPermission) Error() string {
	principalName := err.Principal.GetName()
	reasons := make([]string, len(err.Reasons), len(err.Reasons))
	for i, reason := range err.Reasons {
		reasons[i] = fmt.Sprintf("%q %s", principalName, reason)
	}
	return strings.Join(reasons, ", ")
}

// checkQueuePermission checks if the principal embedded in the context has permission
// to perform actions on the given queue. If the principal has sufficient permissions,
// nil is returned. Otherwise an error is returned.
func (server *SubmitServer) checkQueuePermission(
	ctx context.Context,
	queueName string,
	attemptToCreate bool,
	basicPermission permission.Permission,
	allQueuesPermission permission.Permission) ([]string, error) {

	// Load the q into memory to check if the user is the owner of the q
	q, err := server.queueRepository.GetQueue(queueName)
	var e *repository.ErrQueueNotFound

	// TODO Checking permissions shouldn't have side side effects.
	// Hence, this function shouldn't automatically create queues.
	// Further, we should consider removing the AutoCreateQueues option entirely.
	// Since it leads to surprising behavior (e.g., creating a queue if the name is misspelled).
	// Creating queues should always be an explicit decision.
	// The less surprising behavior is to return ErrQueueNotFound (perhaps wrapped).
	if errors.As(err, &e) && attemptToCreate && server.queueManagementConfig.AutoCreateQueues {
		// TODO Is this correct? Shouldn't the relevant permission be permissions.CreateQueue?
		err = checkPermission(server.permissions, ctx, permissions.SubmitAnyJobs)
		if err != nil {
			return nil, fmt.Errorf("[checkQueuePermission] error: %w", err)
		}

		q = queue.Queue{
			Name:           queueName,
			PriorityFactor: server.queueManagementConfig.DefaultPriorityFactor,
		}
		err = server.queueRepository.CreateQueue(q)
		if err != nil {
			return nil, fmt.Errorf("[checkQueuePermission] error creating queue: %w", err)
		}

		// nil indicates that the user has sufficient permissions
		// The newly created group has no ownership groups
		return []string{}, nil
	} else if err != nil {
		return nil, fmt.Errorf("[checkQueuePermission] error getting queue %s: %w", queueName, err)
	}

	// The user must either own the queue or have permission to access all queues
	permissionToCheck := basicPermission
	owned, groups := server.permissions.UserOwns(ctx, q.ToAPI())
	if !owned {
		permissionToCheck = allQueuesPermission
	}
	if err := checkPermission(server.permissions, ctx, permissionToCheck); err != nil {
		err = &ErrNoPermission{
			Principal: authorization.GetPrincipal(ctx),
			Reasons: []string{
				fmt.Sprintf("does not own queue %q and have %s permissions for it", queueName, basicPermission),
				fmt.Sprintf("does not have %s permissions", allQueuesPermission),
			},
		}
		return nil, err
	}
	return groups, nil
}

// checkPermission is a helper function called by the gRPC handlers to check if a client has the
// permissions required to perform some action. The error returned is of type ErrNoPermission.
// After recovering the error (using errors.As), the caller can obtain the name of the user and the
// requested permission programatically via this error type.
func checkPermission(p authorization.PermissionChecker, ctx context.Context, permission permission.Permission) error {
	if !p.UserHasPermission(ctx, permission) {
		return &ErrNoPermission{
			Principal: authorization.GetPrincipal(ctx),
			Reasons: []string{
				fmt.Sprintf("does not have permission %s", permission),
			},
		}
	}
	return nil
}
