package handlers

import (
	"context"

	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/validation"
	"github.com/G-Research/armada/pkg/api"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type queueOwnedBy func(ctx context.Context, queueName string) (bool, error)

type submitJobs func(context.Context, *api.JobSubmitRequest) (*api.JobSubmitResponse, error)

func (submit submitJobs) Validate(validateItem validation.JobSubmitRequestItemFn) submitJobs {
	return func(ctx context.Context, request *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
		if request.Queue == "" {
			return nil, status.Errorf(codes.InvalidArgument, "queue is not specified")
		}
		if request.JobSetId == "" {
			return nil, status.Errorf(codes.InvalidArgument, "job set is not specified")
		}

		for index, item := range request.JobRequestItems {
			if err := validateItem(item); err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "Job with index %d is invalid: %s", index, err)
			}
		}

		return submit(ctx, request)
	}
}

func (submit submitJobs) Authorize(ownedBy queueOwnedBy, authorize authorize, autocreate bool) submitJobs {
	return func(ctx context.Context, request *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
		owned, err := ownedBy(ctx, request.Queue)
		switch {
		case authorize(ctx, permissions.SubmitAnyJobs):
			if err == repository.ErrQueueNotFound && !autocreate {
				return nil, status.Errorf(codes.NotFound, "queue doesn't exist")
			}
			if err != repository.ErrQueueNotFound && err != nil {
				return nil, status.Errorf(codes.Internal, "%s", err)
			}
			return submit(ctx, request)
		case authorize(ctx, permissions.SubmitJobs):
			if err == repository.ErrQueueNotFound {
				return nil, status.Errorf(codes.NotFound, "queue: %s doesn't exist", request.Queue)
			}
			if err != nil {
				return nil, status.Errorf(codes.Internal, "%s", err)
			}
			if !owned {
				return nil, status.Errorf(codes.PermissionDenied, "User is not owner of queue: %s", request.Queue)
			}
			return submit(ctx, request)
		default:
			return nil, status.Errorf(codes.PermissionDenied, "User doesn't have permissions: %s or %s", permissions.SubmitJobs, permissions.SubmitAnyJobs)
		}
	}

}

func SubmitJobs(newJobs func(context.Context, *api.JobSubmitRequest) ([]*api.Job, error), addJobs func([]*api.Job) (repository.SubmitJobResults, error)) submitJobs {
	return func(ctx context.Context, request *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
		jobs, err := newJobs(ctx, request)
		if err == repository.ErrQueueNotFound {
			return nil, status.Errorf(codes.NotFound, "queue doesn't exist")
		}
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to generete jobs from request")
		}

		submittedJobs, err := addJobs(jobs)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to submit jobs")
		}

		return newSubmitResponse(submittedJobs), nil
	}
}

func newSubmitResponse(submittedJobs repository.SubmitJobResults) *api.JobSubmitResponse {
	submittedItems := make([]*api.JobSubmitResponseItem, len(submittedJobs))
	for index, submissionResult := range submittedJobs {
		errString := ""
		if submissionResult.Error != nil {
			errString = submissionResult.Error.Error()
		}
		submittedItems[index] = &api.JobSubmitResponseItem{
			JobId: submissionResult.JobId,
			Error: errString,
		}
	}

	return &api.JobSubmitResponse{
		JobResponseItems: submittedItems,
	}
}
