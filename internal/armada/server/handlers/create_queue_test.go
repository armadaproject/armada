package handlers

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/pkg/api"

	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateQueue(t *testing.T) {
	properties := map[string]interface{}{
		"failure": func(request api.Queue) bool {
			createQueue := func(*api.Queue) error {
				return fmt.Errorf("failed to create queue")
			}

			handler := CreateQueue(createQueue)

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
			createQueue := func(*api.Queue) error {
				return &repository.ErrQueueAlreadyExists{queueName: "foo"}
			}

			handler := CreateQueue(createQueue)
			_, err := handler(context.Background(), &request)
			if err == nil {
				t.Errorf("failed to handle an error")
				return false
			}
			if s, _ := status.FromError(err); s.Code() != codes.AlreadyExists {
				t.Errorf("invalid status code")
				return false
			}

			return true
		},
		"success": func(request api.Queue) bool {
			createQueue := func(queue *api.Queue) error {
				if !reflect.DeepEqual(queue, &request) {
					return fmt.Errorf("invalid queue data")
				}
				return nil
			}

			handler := CreateQueue(createQueue)
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

func TestCreateQueueAuthorize(t *testing.T) {

	properties := map[string]interface{}{
		"passtrough": func(fail bool, req api.Queue, perm permission.Permission) bool {
			authorize := func(_ context.Context, permInput permission.Permission) bool {
				return permInput == perm
			}

			handler1 := createQueueHandler(func(_ context.Context, request *api.Queue) (*types.Empty, error) {
				if fail {
					return nil, fmt.Errorf("failed to handle")
				}
				if !reflect.DeepEqual(req, request) {
					return nil, fmt.Errorf("invalid request")
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
		"unauthorized": func(req api.Queue, perm permission.Permission) bool {
			authorize := func(context.Context, permission.Permission) bool {
				return false
			}

			handler := createQueueHandler(func(context.Context, *api.Queue) (*types.Empty, error) {
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

func TestCreateQueueValidate(t *testing.T) {
	properties := map[string]interface{}{
		"passtrough": func(fail bool, req api.Queue) bool {
			if req.PriorityFactor < 1.0 {
				req.PriorityFactor = 1.0
			}

			handler1 := createQueueHandler(func(_ context.Context, request *api.Queue) (*types.Empty, error) {
				if fail {
					return nil, fmt.Errorf("failed to handle")
				}
				if !reflect.DeepEqual(req, request) {
					return nil, fmt.Errorf("invalid request")
				}
				return nil, nil
			})

			handler2 := handler1.Validate()

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
		"invalid": func(req api.Queue) bool {
			if req.PriorityFactor >= 1.0 {
				req.PriorityFactor = 0
			}

			handler := createQueueHandler(func(context.Context, *api.Queue) (*types.Empty, error) {
				return nil, nil
			}).Validate()

			_, err := handler(context.Background(), &req)
			if err == nil {
				t.Errorf("failed to handle an error")
				return false
			}
			if s, _ := status.FromError(err); s.Code() != codes.InvalidArgument {
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
func TestCreateQueueOwnership(t *testing.T) {
	properties := map[string]interface{}{
		"passtrough": func(fail bool, req api.Queue, ownership string) bool {
			if req.UserOwners == nil {
				req.GroupOwners = []string{ownership}
			}

			handler1 := createQueueHandler(func(_ context.Context, request *api.Queue) (*types.Empty, error) {
				if fail {
					return nil, fmt.Errorf("failed to handle")
				}
				if !reflect.DeepEqual(req, request) {
					return nil, fmt.Errorf("invalid request")
				}
				return nil, nil
			})

			handler2 := handler1.Ownership(ownership)

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
		"ownership": func(req api.Queue, owner string) bool {
			req.UserOwners = nil
			handler := createQueueHandler(func(_ context.Context, request *api.Queue) (*types.Empty, error) {
				req.UserOwners = []string{owner}
				if !reflect.DeepEqual(request, &req) {
					return nil, fmt.Errorf("invalid request")
				}
				return nil, nil
			}).Ownership(owner)

			if _, err := handler(context.Background(), &req); err != nil {
				t.Errorf("failed to handle request: %s", err)
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
