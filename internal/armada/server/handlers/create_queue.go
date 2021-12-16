package handlers

import (
	"context"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/pkg/api"

	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type create func(*api.Queue) error
type createQueueHandler func(context.Context, *api.Queue) (*types.Empty, error)

func (create createQueueHandler) Authorize(authorize authorize, perm permission.Permission) createQueueHandler {
	return func(ctx context.Context, request *api.Queue) (*types.Empty, error) {
		if !authorize(ctx, perm) {
			return nil, status.Errorf(codes.PermissionDenied, "User has no permission: %s", perm)
		}
		return create(ctx, request)
	}
}

func (create createQueueHandler) Validate() createQueueHandler {
	return func(ctx context.Context, request *api.Queue) (*types.Empty, error) {
		if request.PriorityFactor < 1.0 {
			return nil, status.Errorf(codes.InvalidArgument, "Minimum queue priority factor is 1.")
		}
		return create(ctx, request)
	}
}

func (create createQueueHandler) Ownership(owner string) createQueueHandler {
	return func(ctx context.Context, request *api.Queue) (*types.Empty, error) {
		if len(request.UserOwners) == 0 {
			request.UserOwners = []string{owner}
		}
		return create(ctx, request)
	}
}

func CreateQueue(create create) createQueueHandler {
	return func(ctx context.Context, queue *api.Queue) (*types.Empty, error) {
		err := create(queue)
		switch {
		case err == repository.ErrQueueAlreadyExists:
			return nil, status.Errorf(codes.AlreadyExists, "Queue %q already exists", queue.Name)
		case err != nil:
			return nil, status.Errorf(codes.Unavailable, err.Error())
		default:
			return &types.Empty{}, nil
		}
	}
}
