package server

import (
	"context"
	"math"
	"time"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
	"github.com/gogo/protobuf/types"
	"github.com/gogo/status"
	pool "github.com/jolestar/go-commons-pool"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
)

type SubmitServer struct {
	authorizer            ActionAuthorizer
	jobRepository         repository.JobRepository
	queueRepository       repository.QueueRepository
	cancelJobsBatchSize   int
	queueManagementConfig *configuration.QueueManagementConfig
	schedulingConfig      *configuration.SchedulingConfig
	compressorPool        *pool.ObjectPool
}

func NewSubmitServer(
	authorizer ActionAuthorizer,
	jobRepository repository.JobRepository,
	queueRepository repository.QueueRepository,
	cancelJobsBatchSize int,
	queueManagementConfig *configuration.QueueManagementConfig,
	schedulingConfig *configuration.SchedulingConfig,
) *SubmitServer {
	poolConfig := pool.ObjectPoolConfig{
		MaxTotal:                 100,
		MaxIdle:                  50,
		MinIdle:                  10,
		BlockWhenExhausted:       true,
		MinEvictableIdleTime:     30 * time.Minute,
		SoftMinEvictableIdleTime: math.MaxInt64,
		TimeBetweenEvictionRuns:  0,
		NumTestsPerEvictionRun:   10,
	}

	compressorPool := pool.NewObjectPool(armadacontext.Background(), pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			return compress.NewZlibCompressor(512)
		}), &poolConfig)

	return &SubmitServer{
		authorizer:            authorizer,
		jobRepository:         jobRepository,
		queueRepository:       queueRepository,
		cancelJobsBatchSize:   cancelJobsBatchSize,
		queueManagementConfig: queueManagementConfig,
		schedulingConfig:      schedulingConfig,
		compressorPool:        compressorPool,
	}
}

func (server *SubmitServer) Health(ctx context.Context, _ *types.Empty) (*api.HealthCheckResponse, error) {
	// For now, lets make the health check really simple.
	return &api.HealthCheckResponse{Status: api.HealthCheckResponse_SERVING}, nil
}

func (server *SubmitServer) GetQueue(grpcCtx context.Context, req *api.QueueGetRequest) (*api.Queue, error) {
	queue, err := server.queueRepository.GetQueue(req.Name)
	var e *repository.ErrQueueNotFound
	if errors.As(err, &e) {
		return nil, status.Errorf(codes.NotFound, "[GetQueue] error: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[GetQueue] error getting queue %q: %s", req.Name, err)
	}
	return queue.ToAPI(), nil
}

func (server *SubmitServer) GetQueues(req *api.StreamingQueueGetRequest, stream api.Submit_GetQueuesServer) error {
	// Receive once to get information about the number of queues to return
	numToReturn := req.GetNum()
	if numToReturn < 1 {
		numToReturn = math.MaxUint32
	}

	queues, err := server.queueRepository.GetAllQueues()
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

func (server *SubmitServer) CreateQueue(grpcCtx context.Context, request *api.Queue) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := server.authorizer.AuthorizeAction(ctx, permissions.CreateQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "[CreateQueue] error creating queue %s: %s", request.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[CreateQueue] error checking permissions: %s", err)
	}

	if len(request.UserOwners) == 0 {
		principal := authorization.GetPrincipal(ctx)
		request.UserOwners = []string{principal.GetName()}
	}

	queue, err := queue.NewQueue(request)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[CreateQueue] error validating queue: %s", err)
	}

	err = server.queueRepository.CreateQueue(queue)
	var eq *repository.ErrQueueAlreadyExists
	if errors.As(err, &eq) {
		return nil, status.Errorf(codes.AlreadyExists, "[CreateQueue] error creating queue: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[CreateQueue] error creating queue: %s", err)
	}

	return &types.Empty{}, nil
}

func (server *SubmitServer) CreateQueues(grpcCtx context.Context, request *api.QueueList) (*api.BatchQueueCreateResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	var failedQueues []*api.QueueCreateResponse
	// Create a queue for each element of the request body and return the failures.
	for _, queue := range request.Queues {
		_, err := server.CreateQueue(ctx, queue)
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

func (server *SubmitServer) UpdateQueue(grpcCtx context.Context, request *api.Queue) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := server.authorizer.AuthorizeAction(ctx, permissions.CreateQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "[UpdateQueue] error updating queue %s: %s", request.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[UpdateQueue] error checking permissions: %s", err)
	}

	queue, err := queue.NewQueue(request)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[UpdateQueue] error: %s", err)
	}

	err = server.queueRepository.UpdateQueue(queue)
	var e *repository.ErrQueueNotFound
	if errors.As(err, &e) {
		return nil, status.Errorf(codes.NotFound, "[UpdateQueue] error: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[UpdateQueue] error getting queue %q: %s", queue.Name, err)
	}

	return &types.Empty{}, nil
}

func (server *SubmitServer) UpdateQueues(grpcCtx context.Context, request *api.QueueList) (*api.BatchQueueUpdateResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	var failedQueues []*api.QueueUpdateResponse

	// Create a queue for each element of the request body and return the failures.
	for _, queue := range request.Queues {
		_, err := server.UpdateQueue(ctx, queue)
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

func (server *SubmitServer) DeleteQueue(grpcCtx context.Context, request *api.QueueDeleteRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := server.authorizer.AuthorizeAction(ctx, permissions.DeleteQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "[DeleteQueue] error deleting queue %s: %s", request.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[DeleteQueue] error checking permissions: %s", err)
	}
	err = server.queueRepository.DeleteQueue(request.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[DeleteQueue] error deleting queue %s: %s", request.Name, err)
	}
	return &types.Empty{}, nil
}
