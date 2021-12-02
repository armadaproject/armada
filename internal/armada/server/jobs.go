package server

import (
	"context"
	"fmt"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/pkg/api"
)

type newJobsFn func(ctx context.Context, req *api.JobSubmitRequest) ([]*api.Job, error)

type getClusterSchedulingInfo func() (map[string]*api.ClusterSchedulingInfoReport, error)
type validateJobs func(jobs []*api.Job, allClusterSchedulingInfo map[string]*api.ClusterSchedulingInfoReport) error

func (new newJobsFn) Validate(getSchedulingInfo getClusterSchedulingInfo, validateJobs validateJobs) newJobsFn {
	return func(ctx context.Context, req *api.JobSubmitRequest) ([]*api.Job, error) {
		jobs, err := new(ctx, req)
		if err != nil {
			return nil, err
		}

		schedulingInfo, err := getSchedulingInfo()
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve cluster scheduling info: %w", err)
		}

		if err := validateJobs(jobs, schedulingInfo); err != nil {
			return nil, fmt.Errorf("failed to validate jobs scheduling: %w", err)
		}

		return jobs, nil
	}
}

func NewJobs(owner string, getQueueOwnership GetQueueOwnershipFn, fromRequest api.JobsFromSubmitRequestFn) newJobsFn {
	return func(ctx context.Context, req *api.JobSubmitRequest) ([]*api.Job, error) {
		ownership, err := getQueueOwnership(ctx, req.Queue)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve queue ownership. %w", err)
		}

		return fromRequest(req, owner, ownership), nil
	}
}

type GetQueueOwnershipFn func(context.Context, string) ([]string, error)

func GetQueueOwnership(getQueue repository.GetQueueFn, getOwnership func(context.Context, authorization.Owned) (bool, []string)) GetQueueOwnershipFn {
	return func(ctx context.Context, queueName string) ([]string, error) {
		queue, err := getQueue(queueName)
		if err != nil {
			return nil, err
		}

		_, groups := getOwnership(ctx, queue)

		return groups, nil
	}
}
