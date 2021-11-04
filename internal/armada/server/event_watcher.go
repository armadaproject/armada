package server

import (
	"context"
	"fmt"
	"time"

	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/pkg/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readEvents func(queue, jobSetId string, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error)
type authorize func(context.Context, permission.Permission) bool
type sendEvent func(*api.EventStreamMessage) error

type EventWatcher func(context.Context, *api.WatchRequest) error

// Authorize validates if the user has
func (watcher EventWatcher) Authorize(authorize authorize, perm permission.Permission) EventWatcher {
	return func(ctx context.Context, request *api.WatchRequest) error {
		if !authorize(ctx, perm) {
			return status.Errorf(codes.PermissionDenied, "User has no permission: %s", perm)
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
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			events, err := read(request.Queue, request.JobSetId, request.FromId, 500, 5*time.Second)
			if err != nil {
				return fmt.Errorf("failed to read event from repository. %s", err)
			}

			for _, event := range events {
				if err := send(event); err != nil {
					return fmt.Errorf("failed to send event over the stream. %s", err)
				}
			}
		}
	}
}
