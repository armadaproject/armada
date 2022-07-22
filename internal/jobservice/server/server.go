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
	response, err := s.jobRepository.GetJobStatus(opts.JobId)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	if !s.jobRepository.IsJobSetAlreadySubscribed(opts.JobSetId) {

		eventJob := eventstojobs.NewEventsToJobService(opts.Queue, opts.JobSetId, opts.JobId, s.jobServiceConfig, s.jobRepository)
		go eventJob.SubscribeToJobSetId(context.Background())
	}

	return response, err
}
