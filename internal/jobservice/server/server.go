package server

import (
	"context"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/internal/jobservice/eventstojobs"
	"github.com/G-Research/armada/internal/jobservice/repository"

	js "github.com/G-Research/armada/pkg/api/jobservice"
)

type JobServiceServer struct {
	jobServiceConfig *configuration.JobServiceConfiguration
	jobRepository    *repository.InMemoryJobServiceRepository
}

func NewJobService(config *configuration.JobServiceConfiguration, inMemoryService  *repository.InMemoryJobServiceRepository) *JobServiceServer {
	return &JobServiceServer{jobServiceConfig: config, jobRepository: inMemoryService}
}

func (s *JobServiceServer) GetJobStatus(ctx context.Context, opts *js.JobServiceRequest) (*js.JobServiceResponse, error) {
	// We want to support cases where cache doesn't exist
	if s.jobServiceConfig.SkipRedisCache || !s.jobRepository.HealthCheck() {
		jobResponse, err := s.GetJobStatusWithNoRedis(context.Background(), opts)
		if err != nil {
			log.Fatal(err)
		}
		return jobResponse, err
	}
	response, err := s.jobRepository.GetJobStatus(opts.JobId)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	var subscribeToJobSet = eventstojobs.IsStateTerminal(response.State)
	if !subscribeToJobSet {

		eventJob := eventstojobs.NewEventsToJobService(opts.Queue, opts.JobSetId, opts.JobId, s.jobServiceConfig, s.jobRepository)
		eventJob.SubscribeToJobSetId(ctx)
		return s.jobRepository.GetJobStatus(opts.JobId)
	}

	return response, err
}

func (s *JobServiceServer) GetJobStatusWithNoRedis(ctx context.Context, opts *js.JobServiceRequest) (*js.JobServiceResponse, error) {
	eventJob := eventstojobs.NewEventsToJobService(opts.Queue, opts.JobSetId, opts.JobId, s.jobServiceConfig, s.jobRepository)
	return eventJob.GetStatusWithoutRedis(ctx, opts.JobId)
}
