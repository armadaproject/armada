package server

import (
	"context"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/pkg/api"
)

type CreateQueueFn func(*api.Queue) error
type GetQueueFn func(string) (*api.Queue, error)

func (get GetQueueFn) Autocreate(autocreate bool, priorityFactor float64, create CreateQueueFn) GetQueueFn {
	return func(queueName string) (*api.Queue, error) {
		queue, err := get(queueName)
		switch {
		case !autocreate, err == nil, err != repository.ErrQueueNotFound:
			return queue, err
		default:
			queue = &api.Queue{
				Name:           queueName,
				PriorityFactor: priorityFactor,
			}

			if err := create(queue); err != nil {
				return nil, err
			}

			return queue, nil
		}
	}
}

type GetQueueOwnershipFn func(context.Context, string) ([]string, error)

func GetQueueOwnership(getQueue GetQueueFn, getOwnership func(context.Context, authorization.Owned) (bool, []string)) GetQueueOwnershipFn {
	return func(ctx context.Context, queueName string) ([]string, error) {
		queue, err := getQueue(queueName)
		if err != nil {
			return nil, err
		}

		_, groups := getOwnership(ctx, queue)

		return groups, nil
	}
}
