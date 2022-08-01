package server

import (
	"context"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/internal/jobservice/eventstojobs"
	"github.com/G-Research/armada/internal/jobservice/repository"

	js "github.com/G-Research/armada/pkg/api/jobservice"
)

type JobServiceServer struct {
	jobServiceConfig *configuration.JobServiceConfiguration
	jobRepository    repository.InMemoryJobServiceRepository
}

func NewJobService(config *configuration.JobServiceConfiguration, inMemoryService repository.InMemoryJobServiceRepository) *JobServiceServer {
	return &JobServiceServer{jobServiceConfig: config, jobRepository: inMemoryService}
}

func (s *JobServiceServer) GetJobStatus(ctx context.Context, opts *js.JobServiceRequest) (*js.JobServiceResponse, error) {
	g, _ := errgroup.WithContext(ctx)

	queueJobSetKey := opts.Queue + opts.JobSetId
	if !s.jobRepository.IsJobSetSubscribed(queueJobSetKey) {

		eventJob := eventstojobs.NewEventsToJobService(opts.Queue, opts.JobSetId, opts.JobId, s.jobServiceConfig, s.jobRepository)
		g.Go(func() error {
			return eventJob.SubscribeToJobSetId(context.Background())
		})
	}
	s.jobRepository.UpdateJobSetTime(queueJobSetKey)
	response, err := s.jobRepository.GetJobStatus(opts.JobId)
	if err != nil {
		log.Warn(err)
		return nil, err
	}

	return response, err
}
