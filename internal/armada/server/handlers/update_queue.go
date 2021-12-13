package handlers

import (
	"context"
	"errors"

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
		var e *repository.ErrQueueNotFound
		if errors.As(err, &e) {
			return nil, status.Errorf(codes.NotFound, "error updating queue %q: not found", queue.Name)
		} else if err != nil {
			return nil, status.Errorf(codes.Unavailable, "error updating queue %q: %s", queue.Name, err)
		}
		return &types.Empty{}, nil
	}
}
