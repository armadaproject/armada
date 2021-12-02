package handlers

import (
	"context"

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

		switch {
		case err == repository.ErrQueueNotFound:
			return nil, status.Errorf(codes.NotFound, "Queue %q not found", req.Name)
		case err != nil:
			return nil, status.Errorf(codes.Unavailable, "Could not load queue %q: %s", req.Name, err.Error())
		default:
			return queue, nil
		}
	}
}
