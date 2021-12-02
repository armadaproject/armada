package server

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/pkg/api"
)

func TestNewJobs(t *testing.T) {
	properties := map[string]interface{}{
		"success": func(req api.JobSubmitRequest, principal string, owners []string) bool {
			new := NewJobs(
				principal,
				func(c context.Context, queueName string) ([]string, error) {
					if queueName != req.Queue {
						return nil, fmt.Errorf("invalid queue name")
					}
					return owners, nil
				},
				func(request *api.JobSubmitRequest, owner string, ownershipGroups []string) []*api.Job {
					switch {
					case !reflect.DeepEqual(request, &req):
						t.Errorf("invalid request data")
					case owner != principal:
						t.Errorf("invalid owner")
					case !reflect.DeepEqual(ownershipGroups, owners):
						t.Errorf("invalid ownership groups")
					default:
					}
					return nil
				},
			)

			if _, err := new(context.Background(), &req); err != nil {
				t.Errorf("failed to generate jobs from request: %s", err)
				return false
			}

			return true
		},
		"erorr": func(req api.JobSubmitRequest, principal string) bool {
			new := NewJobs(
				principal,
				func(c context.Context, queueName string) ([]string, error) {
					return nil, fmt.Errorf("")
				},
				func(request *api.JobSubmitRequest, owner string, ownershipGroups []string) []*api.Job {
					return nil
				},
			)

			if _, err := new(context.Background(), &req); err == nil {
				t.Errorf("failed handle an error")
				return false
			}

			return true
		},
	}

	for name, property := range properties {
		t.Run(name, func(tb *testing.T) {
			if err := quick.Check(property, nil); err != nil {
				tb.Fatal(err)
			}
		})
	}
}

func TestNewJobsValidate(t *testing.T) {
	properties := map[string]interface{}{
		"success": func(req api.JobSubmitRequest) bool {
			new := newJobsFn(func(ctx context.Context, req *api.JobSubmitRequest) ([]*api.Job, error) {
				return nil, nil
			}).Validate(
				func() (map[string]*api.ClusterSchedulingInfoReport, error) {
					return nil, nil
				},
				func(jobs []*api.Job, allClusterSchedulingInfo map[string]*api.ClusterSchedulingInfoReport) error {
					return nil
				},
			)

			if _, err := new(context.Background(), &req); err != nil {
				t.Errorf("failed to generate jobs from request: %s", err)
				return false
			}

			return true
		},
		"erorr": func(req api.JobSubmitRequest, newJobsError, schedulingError, validationError bool) bool {
			new := newJobsFn(func(ctx context.Context, req *api.JobSubmitRequest) ([]*api.Job, error) {
				if newJobsError {
					return nil, fmt.Errorf("failed to generate jobs from request")
				}
				return nil, nil
			}).Validate(
				func() (map[string]*api.ClusterSchedulingInfoReport, error) {
					if schedulingError {
						return nil, fmt.Errorf("failed to retrieve scheduling info")
					}
					return nil, nil
				},
				func(jobs []*api.Job, allClusterSchedulingInfo map[string]*api.ClusterSchedulingInfoReport) error {
					if validationError {
						return fmt.Errorf("job validation failed")
					}
					return nil
				},
			)

			isError := newJobsError || schedulingError || validationError

			if _, err := new(context.Background(), &req); err == nil && isError {
				t.Errorf("failed handle an error")
				return false
			}

			return true
		},
	}

	for name, property := range properties {
		t.Run(name, func(tb *testing.T) {
			if err := quick.Check(property, nil); err != nil {
				tb.Fatal(err)
			}
		})
	}
}

func TestGetQueueOwnership(t *testing.T) {
	properties := map[string]interface{}{
		"success": func(qname string, qeueu api.Queue, groups []string) bool {
			getOwnership := GetQueueOwnership(
				func(queueName string) (*api.Queue, error) {
					if queueName != qname {
						return nil, fmt.Errorf("invalid queue name")
					}
					return &qeueu, nil
				},
				func(c context.Context, o authorization.Owned) (bool, []string) {
					return true, groups
				},
			)

			ownerships, err := getOwnership(context.Background(), qname)
			if err != nil {
				t.Errorf("failed to retrieve queue ownership: %s", err)
				return false
			}
			return reflect.DeepEqual(ownerships, groups)
		},
		"error": func(qname string) bool {
			getOwnership := GetQueueOwnership(
				func(queueName string) (*api.Queue, error) {
					return nil, fmt.Errorf("")
				},
				func(c context.Context, o authorization.Owned) (bool, []string) {
					return true, nil
				},
			)

			if _, err := getOwnership(context.Background(), qname); err == nil {
				t.Errorf("failed to handle an error")
				return false
			}
			return true
		},
	}

	for name, property := range properties {
		t.Run(name, func(tb *testing.T) {
			if err := quick.Check(property, nil); err != nil {
				tb.Fatal(err)
			}
		})
	}
}
