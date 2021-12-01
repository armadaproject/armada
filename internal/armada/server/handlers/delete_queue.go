package handlers

import (
	"context"

	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/pkg/api"

	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type deleteQueue func(string) error
type deleteQueueHandler func(context.Context, *api.QueueDeleteRequest) (*types.Empty, error)

func (delete deleteQueueHandler) Authorize(authorize authorize, perm permission.Permission) deleteQueueHandler {
	return func(ctx context.Context, request *api.QueueDeleteRequest) (*types.Empty, error) {
		if !authorize(ctx, perm) {
			return nil, status.Errorf(codes.PermissionDenied, "User has no permission: %s", perm)
		}
		return delete(ctx, request)
	}
}

func (delete deleteQueueHandler) CheckIfEmpty(getJobSet getJobSet) deleteQueueHandler {
	return func(ctx context.Context, request *api.QueueDeleteRequest) (*types.Empty, error) {
		active, e := getJobSet(request.Name)
		switch {
		case e != nil:
			return nil, status.Errorf(codes.InvalidArgument, e.Error())
		case len(active) > 0:
			return nil, status.Errorf(codes.FailedPrecondition, "Queue is not empty.")
		default:
			return delete(ctx, request)
		}
	}
}

func DeleteQueue(deleteQueue deleteQueue) deleteQueueHandler {
	return func(ctx context.Context, request *api.QueueDeleteRequest) (*types.Empty, error) {
		if err := deleteQueue(request.Name); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}

		return &types.Empty{}, nil
	}
}
