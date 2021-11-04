package server

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/pkg/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEventWatcher(t *testing.T) {
	properties := map[string]interface{}{
		"Success": func(inputRequest api.WatchRequest) bool {
			ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond*300)
			defer cancel()

			readEvents := func(queue, jobSetId, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error) {
				switch {
				case queue != inputRequest.Queue:
					return nil, fmt.Errorf("Invalid queue")
				case jobSetId != inputRequest.JobSetId:
					return nil, fmt.Errorf("Invalid job set id")
				default:
					return []*api.EventStreamMessage{{}}, nil
				}
			}

			sendEvent := func(esm *api.EventStreamMessage) error {
				return nil
			}

			if err := NewEventWatcher(readEvents, sendEvent)(ctx, &inputRequest); err != nil {
				t.Errorf("failed to process request: %s", err)
				return false
			}
			return true
		},
		"ErrorHandling": func(inputRequest api.WatchRequest, readError, sendError bool) bool {
			ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond*300)
			defer cancel()
			readEvents := func(queue, jobSetId, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error) {
				if readError {
					return nil, fmt.Errorf("")
				}
				return []*api.EventStreamMessage{{}}, nil
			}

			sendEvent := func(esm *api.EventStreamMessage) error {
				if sendError {
					return fmt.Errorf("")
				}
				return nil
			}

			isError := readError || sendError

			if err := NewEventWatcher(readEvents, sendEvent)(ctx, &inputRequest); err == nil && isError {
				t.Errorf("Error handling failed")
				return false
			}

			return true
		},
	}

	for name, property := range properties {
		t.Run(name, func(t *testing.T) {
			if err := quick.Check(property, nil); err != nil {
				t.Error(err)
			}
		})

	}
}
func TestEventWatcherAuthorize(t *testing.T) {
	properties := map[string]interface{}{
		"Autorhized": func(inputRequest api.WatchRequest, inputPermission permission.Permission) bool {
			ctx := context.Background()

			authorize := func(ctx context.Context, permission permission.Permission) bool {
				if permission != inputPermission {
					t.Errorf("Invalid permision, expected: %s", permission)
					return false
				}
				return true
			}

			watcher := EventWatcher(func(ctx context.Context, request *api.WatchRequest) error {
				if !reflect.DeepEqual(request, &inputRequest) {
					t.Errorf("Invalid request")
				}
				return nil
			}).Authorize(authorize, inputPermission)

			if err := watcher(ctx, &inputRequest); err != nil {
				t.Errorf("Request should be authorized")
				return false
			}

			return true
		},
		"Unauthorized": func(inputRequest api.WatchRequest, inputPermission permission.Permission) bool {
			ctx := context.Background()
			authorize := func(ctx context.Context, permission permission.Permission) bool {
				return false
			}

			watcher := EventWatcher(func(ctx context.Context, request *api.WatchRequest) error {
				return nil
			}).Authorize(authorize, inputPermission)

			err := watcher(ctx, &inputRequest)
			if err == nil {
				t.Errorf("Request should fail")
				return false
			}

			if status, _ := status.FromError(err); status.Code() != codes.PermissionDenied {
				t.Errorf("Invalid status code. Expected: %s", codes.PermissionDenied.String())
				return false
			}

			return true
		},
	}

	for name, property := range properties {
		t.Run(name, func(t *testing.T) {
			if err := quick.Check(property, nil); err != nil {
				t.Error(err)
			}
		})

	}
}

func TestEventWatcherMustExists(t *testing.T) {
	properties := map[string]interface{}{
		"Found": func(inputRequest api.WatchRequest) bool {
			ctx := context.Background()

			readEvents := func(queue, jobSetId, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error) {
				switch {
				case queue != inputRequest.Queue:
					return nil, fmt.Errorf("Invalid queue")
				case jobSetId != inputRequest.JobSetId:
					return nil, fmt.Errorf("Invalid job set id")
				default:
					return []*api.EventStreamMessage{{}}, nil
				}
			}

			watcher := EventWatcher(func(ctx context.Context, request *api.WatchRequest) error {
				if !reflect.DeepEqual(request, &inputRequest) {
					return fmt.Errorf("Invalid request")
				}
				return nil
			}).MustExist(readEvents)

			if err := watcher(ctx, &inputRequest); err != nil {
				t.Errorf("Failed to server request: %s", err)
				return false
			}

			return true
		},
		"NotFound": func(inputRequest api.WatchRequest) bool {
			ctx := context.Background()
			readEvents := func(queue, jobSetId, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error) {
				return nil, nil
			}

			watcher := EventWatcher(func(ctx context.Context, request *api.WatchRequest) error {
				return nil
			}).MustExist(readEvents)

			err := watcher(ctx, &inputRequest)
			if err == nil {
				t.Errorf("Request should fail")
				return false
			}

			if status, _ := status.FromError(err); status.Code() != codes.NotFound {
				t.Errorf("Invalid status code. Expected: %s", codes.NotFound.String())
				return false
			}

			return true
		},
		"ErrorHandling": func(inputRequest api.WatchRequest) bool {
			ctx := context.Background()
			readEvents := func(queue, jobSetId, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error) {
				return nil, fmt.Errorf("failed to read events")
			}

			watcher := EventWatcher(func(ctx context.Context, request *api.WatchRequest) error {
				return nil
			}).MustExist(readEvents)

			err := watcher(ctx, &inputRequest)
			if err == nil {
				t.Errorf("Request should fail")
				return false
			}
			return true
		},
	}

	for name, property := range properties {
		t.Run(name, func(t *testing.T) {
			if err := quick.Check(property, nil); err != nil {
				t.Error(err)
			}
		})

	}
}
