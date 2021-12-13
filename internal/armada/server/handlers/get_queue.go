package handlers

import (
	"context"
	"errors"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/pkg/api"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type getQueueHandler func(ctx context.Context, req *api.QueueGetRequest) (*api.Queue, error)

type getQueue func(name string) (*api.Queue, error)

func GetQueue(getQueue getQueue) getQueueHandler {
	return func(ctx context.Context, req *api.QueueGetRequest) (*api.Queue, error) {
		queue, err := getQueue(req.Name)
		var e *repository.ErrQueueNotFound
		if errors.As(err, &e) {
			return nil, status.Errorf(codes.NotFound, "error getting queue %q: not found", req.Name)
		} else if err != nil {
			return nil, status.Errorf(codes.Unavailable, "error getting queue %q: %s", req.Name, err)
		}
		return queue, nil
	}
}
