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
	jobRepository    repository.JobServiceRepository
}

func NewJobService(config *configuration.JobServiceConfiguration, inMemoryService repository.JobServiceRepository) *JobServiceServer {
	return &JobServiceServer{jobServiceConfig: config, jobRepository: inMemoryService}
}

func (s *JobServiceServer) GetJobStatus(ctx context.Context, opts *js.JobServiceRequest) (*js.JobServiceResponse, error) {
	if !s.jobRepository.IsJobSetSubscribed(opts.JobSetId) {

		eventJob := eventstojobs.NewEventsToJobService(opts.Queue, opts.JobSetId, opts.JobId, s.jobServiceConfig, s.jobRepository)
		go eventJob.SubscribeToJobSetId(context.Background())
	}
	response, err := s.jobRepository.GetJobStatus(opts.JobId)
	if err != nil {
		log.Warn(err)
		return nil, err
	}

	return response, err
}
