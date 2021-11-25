package handlers

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/pkg/api"

	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDeleteQueue(t *testing.T) {
	properties := map[string]interface{}{
		"failure": func(request api.QueueDeleteRequest) bool {
			handler := DeleteQueue(func(string) error {
				return fmt.Errorf("failed to delete queue")
			})

			if _, err := handler(context.Background(), &request); err == nil {
				t.Errorf("failed to handle an error")
				return false
			}

			return true
		},
		"success": func(request api.QueueDeleteRequest) bool {
			handler := DeleteQueue(func(name string) error {
				if name != request.Name {
					return fmt.Errorf("invalid queue name")
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

func TestDeleteQueueAuthorize(t *testing.T) {

	properties := map[string]interface{}{
		"passtrough": func(fail bool, req api.QueueDeleteRequest, perm permission.Permission) bool {
			authorize := func(_ context.Context, permInput permission.Permission) bool {
				return permInput == perm
			}

			handler1 := deleteQueueHandler(func(_ context.Context, request *api.QueueDeleteRequest) (*types.Empty, error) {
				if !reflect.DeepEqual(&req, request) {
					return nil, fmt.Errorf("invalid request data")
				}
				return nil, nil
			})

			handler2 := handler1.Authorize(authorize, perm)

			_, err1 := handler1(context.Background(), &req)
			_, err2 := handler2(context.Background(), &req)

			if err1 != nil && err2 != nil {
				return err1.Error() == err2.Error()
			}

			if err1 == nil && err2 == nil {
				return true
			}
			return false
		},
		"unauthorized": func(req api.QueueDeleteRequest, perm permission.Permission) bool {
			authorize := func(context.Context, permission.Permission) bool {
				return false
			}

			handler := deleteQueueHandler(func(context.Context, *api.QueueDeleteRequest) (*types.Empty, error) {
				return nil, nil
			}).Authorize(authorize, perm)

			_, err := handler(context.Background(), &req)
			if err == nil {
				t.Errorf("failed to handle an error")
				return false
			}
			if s, _ := status.FromError(err); s.Code() != codes.PermissionDenied {
				t.Errorf("failed to deny request")
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

func TestDeleteQueueCheckIfEmpty(t *testing.T) {
	properties := map[string]interface{}{
		"passthrough": func(request api.QueueDeleteRequest) bool {
			err := fmt.Errorf("failed to delete queue")

			handler1 := deleteQueueHandler(func(context.Context, *api.QueueDeleteRequest) (*types.Empty, error) {
				return nil, err
			})

			handler2 := handler1.CheckIfEmpty(func(queueName string) ([]*api.JobSetInfo, error) {
				if queueName != request.Name {
					return nil, fmt.Errorf("invalid queue name")
				}
				return nil, nil
			})

			_, err1 := handler1(context.Background(), &request)
			_, err2 := handler2(context.Background(), &request)

			if err1 != nil && err2 != nil {
				return err1.Error() == err2.Error()
			}

			if err1 == nil && err2 == nil {
				return true
			}
			return false
		},
		"queueNotEmpty": func(request api.QueueDeleteRequest, jobSetInfo []*api.JobSetInfo) bool {
			if len(jobSetInfo) == 0 {
				return true
			}

			handler := deleteQueueHandler(func(context.Context, *api.QueueDeleteRequest) (*types.Empty, error) {
				return nil, nil
			}).CheckIfEmpty(func(string) ([]*api.JobSetInfo, error) {
				return jobSetInfo, nil
			})

			_, err := handler(context.Background(), &request)
			if err == nil {
				t.Errorf("failed to handle an error")
				return false
			}
			if s, _ := status.FromError(err); s.Code() != codes.FailedPrecondition {
				t.Errorf("Invalid status code: %s", s.Code())
				return false
			}

			return true
		},
		"failure": func(request api.QueueDeleteRequest) bool {
			handler := deleteQueueHandler(func(context.Context, *api.QueueDeleteRequest) (*types.Empty, error) {
				return nil, nil
			}).CheckIfEmpty(func(string) ([]*api.JobSetInfo, error) {
				return nil, fmt.Errorf("failed to retrieve job set info")
			})

			_, err := handler(context.Background(), &request)
			if err == nil {
				t.Errorf("failed to handle an error")
				return false
			}
			if s, _ := status.FromError(err); s.Code() != codes.InvalidArgument {
				t.Errorf("Invalid status code: %s", err)
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
