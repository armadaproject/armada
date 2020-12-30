package server

import (
	"context"

	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/lookout/repository"
	"github.com/G-Research/armada/pkg/api/lookout"
)

type LookoutServer struct {
	jobRepository repository.JobRepository
}

func NewLookoutServer(jobRepository repository.JobRepository) *LookoutServer {
	return &LookoutServer{jobRepository: jobRepository}
}

func (s *LookoutServer) Overview(ctx context.Context, _ *types.Empty) (*lookout.SystemOverview, error) {
	queues, err := s.jobRepository.GetQueueInfos()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to query queue stats: %s", err)
	}
	return &lookout.SystemOverview{Queues: queues}, nil
}

func (s *LookoutServer) GetJobsInQueue(ctx context.Context, opts *lookout.GetJobsInQueueRequest) (*lookout.GetJobsInQueueResponse, error) {
	jobInfos, err := s.jobRepository.GetJobsInQueue(opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to query jobs in queue: %s", err)
	}
	return &lookout.GetJobsInQueueResponse{JobInfos: jobInfos}, nil
}
