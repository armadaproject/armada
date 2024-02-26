package submit

import (
	"context"
	"math"

	"github.com/gogo/protobuf/types"
	"github.com/gogo/status"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"

	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

func (srv *SubmitServer) CreateQueue(grpcCtx context.Context, req *api.Queue) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := srv.Authorizer.AuthorizeAction(ctx, permissions.CreateQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "[CreateQueue] error creating queue %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[CreateQueue] error checking permissions: %s", err)
	}

	if len(req.UserOwners) == 0 {
		principal := authorization.GetPrincipal(ctx)
		req.UserOwners = []string{principal.GetName()}
	}

	queue, err := queue.NewQueue(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[CreateQueue] error validating queue: %s", err)
	}

	err = srv.QueueRepository.CreateQueue(queue)
	var eq *repository.ErrQueueAlreadyExists
	if errors.As(err, &eq) {
		return nil, status.Errorf(codes.AlreadyExists, "[CreateQueue] error creating queue: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[CreateQueue] error creating queue: %s", err)
	}

	return &types.Empty{}, nil
}

func (srv *SubmitServer) CreateQueues(grpcCtx context.Context, req *api.QueueList) (*api.BatchQueueCreateResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	var failedQueues []*api.QueueCreateResponse
	// Create a queue for each element of the request body and return the failures.
	for _, queue := range req.Queues {
		_, err := srv.CreateQueue(ctx, queue)
		if err != nil {
			failedQueues = append(failedQueues, &api.QueueCreateResponse{
				Queue: queue,
				Error: err.Error(),
			})
		}
	}

	return &api.BatchQueueCreateResponse{
		FailedQueues: failedQueues,
	}, nil
}

func (srv *SubmitServer) UpdateQueue(grpcCtx context.Context, req *api.Queue) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := srv.Authorizer.AuthorizeAction(ctx, permissions.CreateQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "[UpdateQueue] error updating queue %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[UpdateQueue] error checking permissions: %s", err)
	}

	queue, err := queue.NewQueue(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[UpdateQueue] error: %s", err)
	}

	err = srv.QueueRepository.UpdateQueue(queue)
	var e *repository.ErrQueueNotFound
	if errors.As(err, &e) {
		return nil, status.Errorf(codes.NotFound, "[UpdateQueue] error: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[UpdateQueue] error getting queue %q: %s", queue.Name, err)
	}

	return &types.Empty{}, nil
}

func (srv *SubmitServer) UpdateQueues(grpcCtx context.Context, req *api.QueueList) (*api.BatchQueueUpdateResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	var failedQueues []*api.QueueUpdateResponse

	// Create a queue for each element of the request body and return the failures.
	for _, queue := range req.Queues {
		_, err := srv.UpdateQueue(ctx, queue)
		if err != nil {
			failedQueues = append(failedQueues, &api.QueueUpdateResponse{
				Queue: queue,
				Error: err.Error(),
			})
		}
	}

	return &api.BatchQueueUpdateResponse{
		FailedQueues: failedQueues,
	}, nil
}

func (srv *SubmitServer) DeleteQueue(grpcCtx context.Context, req *api.QueueDeleteRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := srv.Authorizer.AuthorizeAction(ctx, permissions.DeleteQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "[DeleteQueue] error deleting queue %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[DeleteQueue] error checking permissions: %s", err)
	}
	err = srv.QueueRepository.DeleteQueue(req.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[DeleteQueue] error deleting queue %s: %s", req.Name, err)
	}
	return &types.Empty{}, nil
}

func (srv *SubmitServer) GetQueue(ctx context.Context, req *api.QueueGetRequest) (*api.Queue, error) {
	queue, err := srv.QueueRepository.GetQueue(req.Name)
	var e *repository.ErrQueueNotFound
	if errors.As(err, &e) {
		return nil, status.Errorf(codes.NotFound, "[GetQueue] error: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[GetQueue] error getting queue %q: %s", req.Name, err)
	}
	return queue.ToAPI(), nil
}

func (srv *SubmitServer) GetQueues(req *api.StreamingQueueGetRequest, stream api.Submit_GetQueuesServer) error {
	// Receive once to get information about the number of queues to return
	numToReturn := req.GetNum()
	if numToReturn < 1 {
		numToReturn = math.MaxUint32
	}

	queues, err := srv.QueueRepository.GetAllQueues()
	if err != nil {
		return err
	}
	for i, queue := range queues {
		if uint32(i) < numToReturn {
			err := stream.Send(&api.StreamingQueueMessage{
				Event: &api.StreamingQueueMessage_Queue{Queue: queue.ToAPI()},
			})
			if err != nil {
				return err
			}
		}
	}
	err = stream.Send(&api.StreamingQueueMessage{
		Event: &api.StreamingQueueMessage_End{
			End: &api.EndMarker{},
		},
	})
	if err != nil {
		return err
	}
	return nil
}
