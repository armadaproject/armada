package queue

import (
	"context"
	"fmt"
	"math"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

type Server struct {
	queueRepository QueueRepository
	authorizer      auth.ActionAuthorizer
}

func NewServer(
	queueRepository QueueRepository,
	authorizer auth.ActionAuthorizer,
) *Server {
	return &Server{
		queueRepository: queueRepository,
		authorizer:      authorizer,
	}
}

func (s *Server) CreateQueue(grpcCtx context.Context, req *api.Queue) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := s.authorizer.AuthorizeAction(ctx, permissions.CreateQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "error creating queue %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
	}

	if len(req.UserOwners) == 0 {
		principal := auth.GetPrincipal(ctx)
		req.UserOwners = []string{principal.GetName()}
	}

	queue, err := queue.NewQueue(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error validating queue: %s", err)
	}

	err = s.queueRepository.CreateQueue(ctx, queue)
	var eq *ErrQueueAlreadyExists
	if errors.As(err, &eq) {
		return nil, status.Errorf(codes.AlreadyExists, "error creating queue: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error creating queue: %s", err)
	}

	return &types.Empty{}, nil
}

func (s *Server) CreateQueues(grpcCtx context.Context, req *api.QueueList) (*api.BatchQueueCreateResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	var failedQueues []*api.QueueCreateResponse
	// Create a queue for each element of the request body and return the failures.
	for _, queue := range req.Queues {
		_, err := s.CreateQueue(ctx, queue)
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

func (s *Server) UpdateQueue(grpcCtx context.Context, req *api.Queue) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := s.authorizer.AuthorizeAction(ctx, permissions.CreateQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "error updating queue %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
	}

	queue, err := queue.NewQueue(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error: %s", err)
	}

	err = s.queueRepository.UpdateQueue(ctx, queue)
	var e *ErrQueueNotFound
	if errors.As(err, &e) {
		return nil, status.Errorf(codes.NotFound, "error: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error getting queue %q: %s", queue.Name, err)
	}

	return &types.Empty{}, nil
}

func (s *Server) UpdateQueues(grpcCtx context.Context, req *api.QueueList) (*api.BatchQueueUpdateResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	var failedQueues []*api.QueueUpdateResponse

	// Create a queue for each element of the request body and return the failures.
	for _, queue := range req.Queues {
		_, err := s.UpdateQueue(ctx, queue)
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

func (s *Server) DeleteQueue(grpcCtx context.Context, req *api.QueueDeleteRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := s.authorizer.AuthorizeAction(ctx, permissions.DeleteQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "error deleting queue %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
	}
	err = s.queueRepository.DeleteQueue(ctx, req.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error deleting queue %s: %s", req.Name, err)
	}
	return &types.Empty{}, nil
}

func (s *Server) GetQueue(grpcCtx context.Context, req *api.QueueGetRequest) (*api.Queue, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	queue, err := s.queueRepository.GetQueue(ctx, req.Name)
	var e *ErrQueueNotFound
	if errors.As(err, &e) {
		return nil, status.Errorf(codes.NotFound, "error: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error getting queue %q: %s", req.Name, err)
	}
	return queue.ToAPI(), nil
}

func (s *Server) GetQueues(req *api.StreamingQueueGetRequest, stream api.QueueService_GetQueuesServer) error {
	ctx := armadacontext.FromGrpcCtx(stream.Context())

	// Receive once to get information about the number of queues to return
	numToReturn := req.GetNum()
	if numToReturn < 1 {
		numToReturn = math.MaxUint32
	}

	queues, err := s.queueRepository.GetAllQueues(ctx)
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

func (s *Server) CordonQueue(grpcCtx context.Context, req *api.QueueCordonRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)

	err := s.authorizer.AuthorizeAction(ctx, permissions.CordonQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "error cordoning queue %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
	}

	queueName := req.Name
	if queueName == "" {
		return nil, fmt.Errorf("cannot cordon queue with empty name")
	}

	return &types.Empty{}, s.queueRepository.CordonQueue(ctx, queueName)
}

func (s *Server) UncordonQueue(grpcCtx context.Context, req *api.QueueUncordonRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)

	err := s.authorizer.AuthorizeAction(ctx, permissions.CordonQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "error uncordoning queue %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
	}

	queueName := req.Name
	if queueName == "" {
		return nil, fmt.Errorf("cannot uncordon queue with empty name")
	}

	return &types.Empty{}, s.queueRepository.UncordonQueue(ctx, queueName)
}
