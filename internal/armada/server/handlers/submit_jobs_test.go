package handlers

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/pkg/api"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSubmitJobs(t *testing.T) {
	properties := map[string]interface{}{
		"notFound": func(request api.JobSubmitRequest) bool {
			handler := SubmitJobs(
				func(_ context.Context, req *api.JobSubmitRequest) ([]*api.Job, error) {
					return nil, repository.ErrQueueNotFound
				},
				func([]*api.Job) (repository.SubmitJobResults, error) {
					return nil, nil
				},
			)

			_, err := handler(context.Background(), &request)
			if err == nil {
				t.Errorf("failed to handle an error")
				return false
			}

			if status, _ := status.FromError(err); status.Code() != codes.NotFound {
				t.Errorf("invalid status code: %s", status.Code())
				return false
			}

			return true
		},
		"internal": func(newJobsError, addJobsError bool, request api.JobSubmitRequest) bool {
			handler := SubmitJobs(
				func(_ context.Context, req *api.JobSubmitRequest) ([]*api.Job, error) {
					if newJobsError {
						return nil, fmt.Errorf("")
					}
					return nil, nil
				},
				func([]*api.Job) (repository.SubmitJobResults, error) {
					if addJobsError {
						return nil, fmt.Errorf("")
					}
					return nil, nil
				},
			)

			isError := newJobsError || addJobsError

			_, err := handler(context.Background(), &request)
			if err == nil && !isError {
				return true
			}

			if status, _ := status.FromError(err); status.Code() != codes.Internal {
				t.Errorf("invalid status code: %s", status.Code())
				return false
			}

			return true
		},
		"success": func(request api.JobSubmitRequest) bool {
			handler := SubmitJobs(
				func(_ context.Context, req *api.JobSubmitRequest) ([]*api.Job, error) {
					return nil, nil
				},
				func(js []*api.Job) (repository.SubmitJobResults, error) {
					return nil, nil
				},
			)

			_, err := handler(context.Background(), &request)
			if err != nil {
				t.Errorf("failed to handle request: %s", err)
			}

			return true
		},
	}

	for name, property := range properties {
		t.Run(name, func(tb *testing.T) {
			if err := quick.Check(property, nil); err != nil {
				tb.Error(err)
			}
		})
	}

}

func TestSubmitJobsValidate(t *testing.T) {
	properties := map[string]interface{}{
		"notFound": func(request api.JobSubmitRequest) bool {
			if len(request.JobRequestItems) == 0 {
				return true
			}

			handler := submitJobs(func(c context.Context, jsr *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
				return nil, nil
			}).Validate(func(item *api.JobSubmitRequestItem) error {
				return fmt.Errorf("")
			})

			_, err := handler(context.Background(), &request)
			if err == nil {
				t.Errorf("failed to handle an error")
				return false
			}

			if status, _ := status.FromError(err); status.Code() != codes.InvalidArgument {
				t.Errorf("invalid status code: %s", status.Code())
				return false
			}

			return true
		},
		"passthrough": func(request api.JobSubmitRequest, response api.JobSubmitResponse) bool {
			request.Queue += "a"
			request.JobSetId += "a"

			handler := submitJobs(func(c context.Context, jsr *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
				if !reflect.DeepEqual(&request, jsr) {
					return nil, fmt.Errorf("invalid request data")
				}
				return &response, nil
			}).Validate(func(item *api.JobSubmitRequestItem) error {
				return nil
			})

			out, err := handler(context.Background(), &request)
			if err != nil {
				t.Errorf("failed to handle request: %s", err)
			}

			return reflect.DeepEqual(&response, out)
		},
	}

	for name, property := range properties {
		t.Run(name, func(tb *testing.T) {
			if err := quick.Check(property, nil); err != nil {
				tb.Error(err)
			}
		})
	}
}

func TestSubmitJobsAuthorize(t *testing.T) {
	properties := map[string]interface{}{
		"notFound": func(request api.JobSubmitRequest, sumbitAnyJob bool) bool {
			handler := submitJobs(func(c context.Context, jsr *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
				return nil, nil
			}).Authorize(
				func(ctx context.Context, queueName string) (bool, error) {
					return false, repository.ErrQueueNotFound
				},
				func(c context.Context, p permission.Permission) bool {
					if sumbitAnyJob {
						return p == permissions.SubmitAnyJobs
					}
					return p == permissions.SubmitJobs
				},
				false,
			)

			_, err := handler(context.Background(), &request)
			if err == nil {
				t.Errorf("failed to handle an error")
				return false
			}

			if status, _ := status.FromError(err); status.Code() != codes.NotFound {
				t.Errorf("invalid status code: %s", status.Code())
				return false
			}

			return true
		},
		"internal": func(request api.JobSubmitRequest, sumbitAnyJob bool) bool {
			handler := submitJobs(func(c context.Context, jsr *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
				return nil, nil
			}).Authorize(
				func(ctx context.Context, queueName string) (bool, error) {
					return false, fmt.Errorf("")
				},
				func(c context.Context, p permission.Permission) bool {
					if sumbitAnyJob {
						return p == permissions.SubmitAnyJobs
					}
					return p == permissions.SubmitJobs
				},
				false,
			)

			_, err := handler(context.Background(), &request)
			if err == nil {
				t.Errorf("failed to handle an error")
				return false
			}

			if status, _ := status.FromError(err); status.Code() != codes.Internal {
				t.Errorf("invalid status code: %s", status.Code())
				return false
			}

			return true
		},
		"permissionDenied": func(request api.JobSubmitRequest, sumbitJob bool) bool {
			handler := submitJobs(func(c context.Context, jsr *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
				return nil, nil
			}).Authorize(
				func(ctx context.Context, queueName string) (bool, error) {
					return false, nil
				},
				func(c context.Context, p permission.Permission) bool {
					return (p == permissions.SubmitJobs) && sumbitJob
				},
				false,
			)

			_, err := handler(context.Background(), &request)
			if err == nil {
				t.Errorf("failed to handle an error")
				return false
			}

			if status, _ := status.FromError(err); status.Code() != codes.PermissionDenied {
				t.Errorf("invalid status code: %s", status.Code())
				return false
			}

			return true
		},
		"success": func(request api.JobSubmitRequest, sumbitAnyJob bool) bool {
			handler := submitJobs(func(c context.Context, jsr *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
				if !reflect.DeepEqual(jsr, &request) {
					return nil, fmt.Errorf("invalid request data")
				}
				return nil, nil
			}).Authorize(
				func(ctx context.Context, queueName string) (bool, error) {
					return true, nil
				},
				func(c context.Context, p permission.Permission) bool {
					if sumbitAnyJob {
						return p == permissions.SubmitAnyJobs
					}
					return p == permissions.SubmitJobs
				},
				false,
			)

			_, err := handler(context.Background(), &request)
			if err != nil {
				t.Errorf("failed to handle request")
				return false
			}

			return true
		},
	}

	for name, property := range properties {
		t.Run(name, func(tb *testing.T) {
			if err := quick.Check(property, nil); err != nil {
				tb.Error(err)
			}
		})
	}
}
