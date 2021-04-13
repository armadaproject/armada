package server

import (
	"context"
	"time"

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
	start := time.Now()
	label := "Overview"
	queues, err := s.jobRepository.GetQueueInfos(ctx)
	elapsed := time.Since(start)
	if err != nil {
		s.metricsCollector.RecordRequestDuration(elapsed, label, metrics.StatusError)
		return nil, status.Errorf(codes.Internal, "failed to query queue stats: %s", err)
	}
	s.metricsCollector.RecordRequestDuration(elapsed, label, metrics.StatusOk)
	return &lookout.SystemOverview{Queues: queues}, nil
}

func (s *LookoutServer) GetJobSets(ctx context.Context, opts *lookout.GetJobSetsRequest) (*lookout.GetJobSetsResponse, error) {
	start := time.Now()
	label := "GetJobSets"
	jobSets, err := s.jobRepository.GetJobSetInfos(ctx, opts)
	elapsed := time.Since(start)
	if err != nil {
		s.metricsCollector.RecordRequestDuration(elapsed, label, metrics.StatusError)
		return nil, status.Errorf(codes.Internal, "failed to query queue stats: %s", err)
	}
	s.metricsCollector.RecordRequestDuration(elapsed, label, metrics.StatusOk)
	return &lookout.GetJobSetsResponse{JobSetInfos: jobSets}, nil
}

func (s *LookoutServer) GetJobs(ctx context.Context, opts *lookout.GetJobsRequest) (*lookout.GetJobsResponse, error) {
	start := time.Now()
	label := "GetJobs"
	jobInfos, err := s.jobRepository.GetJobs(ctx, opts)
	elapsed := time.Since(start)
	if err != nil {
		s.metricsCollector.RecordRequestDuration(elapsed, label, metrics.StatusError)
		return nil, status.Errorf(codes.Internal, "failed to query jobs in queue: %s", err)
	}
	s.metricsCollector.RecordRequestDuration(elapsed, label, metrics.StatusOk)
	return &lookout.GetJobsResponse{JobInfos: jobInfos}, nil
}
