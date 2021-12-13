package handlers

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/pkg/api"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUpdateQueue(t *testing.T) {
	properties := map[string]interface{}{
		"failure": func(request api.Queue) bool {
			handler := UpdateQueue(func(*api.Queue) error {
				return fmt.Errorf("failed to create queue")
			})

			_, err := handler(context.Background(), &request)
			if err == nil {
				t.Errorf("failed to handle an error")
				return false
			}
			if s, _ := status.FromError(err); s.Code() != codes.Unavailable {
				t.Errorf("invalid status code")
				return false
			}

			return true
		},
		"alreadyExists": func(request api.Queue) bool {
			handler := UpdateQueue(func(*api.Queue) error {
				return &repository.ErrQueueNotFound{queueName: "foo"}
			})

			_, err := handler(context.Background(), &request)
			if err == nil {
				t.Errorf("failed to handle an error")
				return false
			}
			if s, _ := status.FromError(err); s.Code() != codes.NotFound {
				t.Errorf("invalid status code")
				return false
			}

			return true
		},
		"success": func(request api.Queue) bool {
			handler := UpdateQueue(func(queue *api.Queue) error {
				if !reflect.DeepEqual(queue, &request) {
					return fmt.Errorf("invalid queue data")
				}
				return nil
			})

			if _, err := handler(context.Background(), &request); err != nil {
				t.Errorf("failed to handle a request: %s", err)
				return false
			}

			return true
		},
	}

	for name, property := range properties {
		t.Run(name, func(st *testing.T) {
			if err := quick.Check(property, nil); err != nil {
				st.Fatal(err)
			}
		})
	}
}
