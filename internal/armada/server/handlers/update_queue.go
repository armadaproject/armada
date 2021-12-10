package handlers

import (
	"context"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/pkg/api"

	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type updateQueue func(*api.Queue) error

func UpdateQueue(update updateQueue) createQueueHandler {
	return func(ctx context.Context, queue *api.Queue) (*types.Empty, error) {
		err := update(queue)
		switch {
		case err == repository.ErrQueueNotFound:
			return nil, status.Errorf(codes.NotFound, "Queue %q not found", queue.Name)
		case err != nil:
			return nil, status.Errorf(codes.Unavailable, err.Error())
		default:
			return &types.Empty{}, nil
		}
	}
}
