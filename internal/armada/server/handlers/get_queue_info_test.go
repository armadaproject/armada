package handlers

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/pkg/api"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetQueueInfo(t *testing.T) {
	properties := map[string]interface{}{
		"failure": func(request api.QueueInfoRequest) bool {
			handler := GetQueueInfo(func(queue string) ([]*api.JobSetInfo, error) {
				return nil, fmt.Errorf("failed to reetrieve queue info")
			})

			if _, err := handler(context.Background(), &request); err == nil {
				t.Errorf("failed to handle an error")
				return false
			}

			return true
		},
		"success": func(request api.QueueInfoRequest, jobSets []*api.JobSetInfo) bool {
			handler := GetQueueInfo(func(queue string) ([]*api.JobSetInfo, error) {
				if queue != request.Name {
					return nil, fmt.Errorf("invalid queue name")
				}
				return jobSets, nil
			})

			result, err := handler(context.Background(), &request)
			if err != nil {
				t.Errorf("failed to handle a request: %s", err)
				return false
			}

			if !reflect.DeepEqual(result, &api.QueueInfo{Name: request.Name, ActiveJobSets: jobSets}) {
				t.Errorf("invalid jobset data")
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

func TestGetQueueInfoAuthorize(t *testing.T) {
	properties := map[string]interface{}{
		"passtrough": func(req api.QueueInfoRequest, perm permission.Permission, queueInfo api.QueueInfo) bool {
			authorize := func(_ context.Context, permInput permission.Permission) bool {
				return permInput == perm
			}

			handler1 := getQueueInfoHandler(func(_ context.Context, request *api.QueueInfoRequest) (*api.QueueInfo, error) {
				if !reflect.DeepEqual(&req, request) {
					return nil, fmt.Errorf("invalid request data")
				}
				return &api.QueueInfo{}, nil
			})

			handler2 := handler1.Authorize(authorize, perm)

			res1, err1 := handler1(context.Background(), &req)
			res2, err2 := handler2(context.Background(), &req)

			if err1 != nil && err2 != nil {
				return err1.Error() == err2.Error()
			}

			if err1 == nil && err2 == nil {
				return reflect.DeepEqual(res1, res2)
			}
			return false
		},
		"unauthorized": func(req api.QueueInfoRequest, perm permission.Permission) bool {
			authorize := func(context.Context, permission.Permission) bool {
				return false
			}

			handler := getQueueInfoHandler(func(context.Context, *api.QueueInfoRequest) (*api.QueueInfo, error) {
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
