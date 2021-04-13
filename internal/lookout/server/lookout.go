package server

import (
	"context"
	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/lookout/metrics"
	"github.com/G-Research/armada/internal/lookout/repository"
	"github.com/G-Research/armada/pkg/api/lookout"
)

type LookoutServer struct {
	jobRepository    repository.JobRepository
	metricsCollector metrics.LookoutCollector
}

func NewLookoutServer(jobRepository repository.JobRepository, metricsCollector metrics.LookoutCollector) *LookoutServer {
	return &LookoutServer{jobRepository: jobRepository, metricsCollector: metricsCollector}
}

func (s *LookoutServer) Overview(ctx context.Context, _ *types.Empty) (*lookout.SystemOverview, error) {
	queues, err := s.jobRepository.GetQueueInfos(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query queue stats: %s", err)
	}
	return &lookout.SystemOverview{Queues: queues}, nil
}

func (s *LookoutServer) GetJobSets(ctx context.Context, opts *lookout.GetJobSetsRequest) (*lookout.GetJobSetsResponse, error) {
	jobSets, err := s.jobRepository.GetJobSetInfos(ctx, opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query queue stats: %s", err)
	}
	return &lookout.GetJobSetsResponse{JobSetInfos: jobSets}, nil
}

func (s *LookoutServer) GetJobs(ctx context.Context, opts *lookout.GetJobsRequest) (*lookout.GetJobsResponse, error) {
	jobInfos, err := s.jobRepository.GetJobs(ctx, opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query jobs in queue: %s", err)
	}
	return &lookout.GetJobsResponse{JobInfos: jobInfos}, nil
}
