package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/pkg/api"
)

// ErrNoPermission represents an error that occurs when a client tries to perform some action
// through the gRPC API for which it does not have permissions. The error contains the name
// of the client and the requested permission.
type ErrNoPermission struct {
	UserName   string
	Permission permission.Permission
}

func (err *ErrNoPermission) Error() string {
	return fmt.Sprintf("user %q lacks permission %q", err.UserName, err.Permission)
}

func (server *SubmitServer) checkReprioritizePerms(ctx context.Context, jobs []*api.Job) error {
	queues := make(map[string]bool)
	for _, job := range jobs {
		queues[job.Queue] = true
	}
	for queue := range queues {
		if _, err := server.checkQueuePermission(ctx, queue, false, permissions.ReprioritizeJobs, permissions.ReprioritizeAnyJobs); err != nil {
			return err
		}
	}
	return nil
}

// checkQueuePermission checks if the principal embedded in the context has permission
// to perform actions on the given queue. If the principal has sufficient permissions,
// nil is returned. Otherwise an error is returned.
//
// TODO We should change the order of the return values.
// The convention is for the error to be the last of the return values.
func (server *SubmitServer) checkQueuePermission(
	ctx context.Context,
	queueName string,
	attemptToCreate bool,
	basicPermission permission.Permission,
	allQueuesPermission permission.Permission) ([]string, error) {

	// Load the queue into memory to check if the user is the owner of the queue
	queue, err := server.queueRepository.GetQueue(queueName)
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

		queue = &api.Queue{
			Name:           queueName,
			PriorityFactor: server.queueManagementConfig.DefaultPriorityFactor,
		}
		err = server.queueRepository.CreateQueue(queue)
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
	//
	// TODO We should have a more specific permission denied error that includes
	// the resource involved (e.g., that it's a queue and the name of the queue),
	// the action attempted (e.g., submitting a job), the the principal (user) name,
	// and the permission required for the action.
	permissionToCheck := basicPermission
	owned, groups := server.permissions.UserOwns(ctx, queue)
	if !owned {
		permissionToCheck = allQueuesPermission
	}
	if err := checkPermission(server.permissions, ctx, permissionToCheck); err != nil {
		return nil, fmt.Errorf("[checkQueuePermission] permission error for queue %s: %w", queueName, err)
	}
	return groups, nil
}

// checkPermission is a helper function called by the gRPC handlers to check if a client has the
// permissions required to perform some action. The error returned is of type ErrNoPermission.
// After recovering the error (using errors.As), the caller can obtain the name of the user and the
// requested permission programatically via this error type.
func checkPermission(p authorization.PermissionChecker, ctx context.Context, permission permission.Permission) error {
	if !p.UserHasPermission(ctx, permission) {
		principal := authorization.GetPrincipal(ctx)
		name := principal.GetName()
		return &ErrNoPermission{UserName: name, Permission: permission}
	}
	return nil
}
