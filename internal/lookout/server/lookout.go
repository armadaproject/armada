package server

import (
	"context"

	"github.com/gogo/protobuf/types"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/lookout/repository"
	"github.com/armadaproject/armada/pkg/api/lookout"
)

type LookoutServer struct {
	jobRepository repository.JobRepository
}

func NewLookoutServer(jobRepository repository.JobRepository) *LookoutServer {
	return &LookoutServer{jobRepository: jobRepository}
}

func (s *LookoutServer) Overview(ctx context.Context, _ *types.Empty) (*lookout.SystemOverview, error) {
	queues, err := s.jobRepository.GetQueueInfos(armadacontext.New(ctx, logrus.NewEntry(logrus.New())))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query queue stats: %s", err)
	}
	return &lookout.SystemOverview{Queues: queues}, nil
}

func (s *LookoutServer) GetJobSets(ctx context.Context, opts *lookout.GetJobSetsRequest) (*lookout.GetJobSetsResponse, error) {
	jobSets, err := s.jobRepository.GetJobSetInfos(armadacontext.New(ctx, logrus.NewEntry(logrus.New())), opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query queue stats: %s", err)
	}
	return &lookout.GetJobSetsResponse{JobSetInfos: jobSets}, nil
}

func (s *LookoutServer) GetJobs(ctx context.Context, opts *lookout.GetJobsRequest) (*lookout.GetJobsResponse, error) {
	jobInfos, err := s.jobRepository.GetJobs(armadacontext.New(ctx, logrus.NewEntry(logrus.New())), opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query jobs in queue: %s", err)
	}
	return &lookout.GetJobsResponse{JobInfos: jobInfos}, nil
}
