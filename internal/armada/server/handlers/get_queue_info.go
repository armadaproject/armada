package handlers

import (
	"context"
	"fmt"

	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/pkg/api"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type authorize func(context.Context, permission.Permission) bool
type getJobSet func(queue string) ([]*api.JobSetInfo, error)

type getQueueInfoHandler func(context.Context, *api.QueueInfoRequest) (*api.QueueInfo, error)

func (get getQueueInfoHandler) Authorize(authorize authorize, perm permission.Permission) getQueueInfoHandler {
	return func(ctx context.Context, request *api.QueueInfoRequest) (*api.QueueInfo, error) {
		if !authorize(ctx, perm) {
			return nil, status.Errorf(codes.PermissionDenied, "User has no permission: %s", perm)
		}
		return get(ctx, request)
	}
}

func GetQueueInfo(getJobSet getJobSet) getQueueInfoHandler {
	return func(ctx context.Context, req *api.QueueInfoRequest) (*api.QueueInfo, error) {
		jobSets, err := getJobSet(req.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve job set for queue: %s. %w", req.Name, err)
		}

		return &api.QueueInfo{
			Name:          req.Name,
			ActiveJobSets: jobSets,
		}, nil
	}
}
