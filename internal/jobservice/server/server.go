package server

import (
	"context"

	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/internal/jobservice/eventstojobs"
	"github.com/G-Research/armada/internal/jobservice/repository"

	"github.com/G-Research/armada/pkg/api/jobservice"
)

type JobServiceServer struct {
	jobServiceConfig *configuration.JobServiceConfiguration
	jobRepository    repository.RedisJobServiceRepository
}

func NewJobService(config *configuration.JobServiceConfiguration, redisService repository.RedisJobServiceRepository) *JobServiceServer {
	return &JobServiceServer{jobServiceConfig: config, jobRepository: redisService}
}

func (s *JobServiceServer) GetJobStatus(ctx context.Context, opts *jobservice.JobServiceRequest) (*jobservice.JobServiceResponse, error) {
	eventJob := eventstojobs.NewEventsToJobService(opts.Queue, opts.JobSetId, opts.JobId, s.jobServiceConfig, &s.jobRepository)
	response, err := s.jobRepository.GetJobStatus(opts.JobId)
	if response.State == jobservice.JobServiceResponse_JOB_ID_NOT_FOUND {
		eventJob.SubscribeToJobSetId(ctx)
		return s.jobRepository.GetJobStatus(opts.JobId)
	}
	return response, err
}
