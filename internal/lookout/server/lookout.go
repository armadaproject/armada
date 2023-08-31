package server

import (
	gocontext "context"

	"github.com/gogo/protobuf/types"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/common/context"
	"github.com/armadaproject/armada/internal/lookout/repository"
	"github.com/armadaproject/armada/pkg/api/lookout"
)

type LookoutServer struct {
	jobRepository repository.JobRepository
}

func NewLookoutServer(jobRepository repository.JobRepository) *LookoutServer {
	return &LookoutServer{jobRepository: jobRepository}
}

func (s *LookoutServer) Overview(ctx gocontext.Context, _ *types.Empty) (*lookout.SystemOverview, error) {
	queues, err := s.jobRepository.GetQueueInfos(context.New(ctx, logrus.NewEntry(logrus.New())))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query queue stats: %s", err)
	}
	return &lookout.SystemOverview{Queues: queues}, nil
}

func (s *LookoutServer) GetJobSets(ctx gocontext.Context, opts *lookout.GetJobSetsRequest) (*lookout.GetJobSetsResponse, error) {
	jobSets, err := s.jobRepository.GetJobSetInfos(context.New(ctx, logrus.NewEntry(logrus.New())), opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query queue stats: %s", err)
	}
	return &lookout.GetJobSetsResponse{JobSetInfos: jobSets}, nil
}

func (s *LookoutServer) GetJobs(ctx gocontext.Context, opts *lookout.GetJobsRequest) (*lookout.GetJobsResponse, error) {
	jobInfos, err := s.jobRepository.GetJobs(context.New(ctx, logrus.NewEntry(logrus.New())), opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query jobs in queue: %s", err)
	}
	return &lookout.GetJobsResponse{JobInfos: jobInfos}, nil
}
