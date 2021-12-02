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

func TestGetQueue(t *testing.T) {
	properties := map[string]interface{}{
		"failure": func(request api.QueueGetRequest) bool {
			handler := GetQueue(func(string) (*api.Queue, error) {
				return nil, fmt.Errorf("failed to delete queue")
			})

			_, err := handler(context.Background(), &request)
			if err == nil {
				t.Errorf("failed to handle an error")
				return false
			}

			if s, _ := status.FromError(err); s.Code() != codes.Unavailable {
				t.Errorf("Invalid status code: %s", s.Code())
				return false
			}

			return true
		},
		"notFound": func(request api.QueueGetRequest) bool {
			handler := GetQueue(func(string) (*api.Queue, error) {
				return nil, repository.ErrQueueNotFound
			})

			_, err := handler(context.Background(), &request)
			if err == nil {
				t.Errorf("failed to handle an error")
				return false
			}

			if s, _ := status.FromError(err); s.Code() != codes.NotFound {
				t.Errorf("Invalid status code: %s", s.Code())
				return false
			}

			return true
		},
		"success": func(request api.QueueGetRequest, queue api.Queue) bool {
			handler := GetQueue(func(string) (*api.Queue, error) {
				return &queue, nil
			})

			result, err := handler(context.Background(), &request)
			if err != nil {
				t.Errorf("failed to handle request. %s", err)
				return false
			}

			if !reflect.DeepEqual(result, &queue) {
				t.Errorf("invalid queue")
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
