package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client/queue"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TODO This file uses the functional approach to writing handlers.
// For consistency, we should change it to use the same style as the others.
type readEvents func(queue, jobSetId string, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error)
type hasPermissions func(authorization.Principal, queue.Queue, queue.PermissionVerb) bool
type getQueue func(queueName string) (queue.Queue, error)
type sendEvent func(*api.EventStreamMessage) error

type EventWatcher func(context.Context, *api.WatchRequest) error

// Authorize validates if the user has
func (watcher EventWatcher) Authorize(getQueue getQueue, hasPermissions hasPermissions) EventWatcher {
	return func(ctx context.Context, request *api.WatchRequest) error {
		principal := authorization.GetPrincipal(ctx)
		q, err := getQueue(request.Queue)
		var expected *repository.ErrQueueNotFound
		if errors.As(err, &expected) {
			return status.Errorf(codes.NotFound, "Queue %s does not exist", request.Queue)
		}
		if err != nil {
			return err
		}

		if !hasPermissions(principal, q, queue.PermissionVerbWatch) {
			return status.Errorf(codes.PermissionDenied, "User %s has no queue permission: %s", principal.GetName(), queue.PermissionVerbWatch)
		}
		return watcher(ctx, request)
	}
}

// MustExist validates if event job set exists. If job set doesn't exists grpc code 'NotFound'
// is returned.
func (watcher EventWatcher) MustExist(readEvents readEvents) EventWatcher {
	return func(ctx context.Context, request *api.WatchRequest) error {
		events, err := readEvents(request.Queue, request.JobSetId, "", 1, 5*time.Second)
		if err != nil {
			return fmt.Errorf("failed to read event from repository. %s", err)
		}
		if len(events) == 0 {
			return status.Errorf(codes.NotFound, "")
		}
		return watcher(ctx, request)
	}
}

// NewEventWatcher creates new event watcher. Event watcher read events from events repository
// and sends events one at the time over the stream.
func NewEventWatcher(read readEvents, send sendEvent) EventWatcher {
	return func(ctx context.Context, request *api.WatchRequest) error {
		fromId := request.FromId
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			events, err := read(request.Queue, request.JobSetId, fromId, 500, 5*time.Second)
			if err != nil {
				return fmt.Errorf("failed to read event from repository. %s", err)
			}

			for _, event := range events {
				if err := send(event); err != nil {
					return fmt.Errorf("failed to send event over the stream. %s", err)
				}
				fromId = event.Id
			}
		}
	}
}
