package server

import (
	"context"

	"github.com/gogo/protobuf/types"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/internal/jobservice/events"
	"github.com/G-Research/armada/internal/jobservice/eventstojobs"
	"github.com/G-Research/armada/internal/jobservice/repository"

	js "github.com/G-Research/armada/pkg/api/jobservice"
)

type JobServiceServer struct {
	jobServiceConfig *configuration.JobServiceConfiguration
	jobRepository    *repository.SQLJobService
}

func NewJobService(config *configuration.JobServiceConfiguration, sqlService *repository.SQLJobService) *JobServiceServer {
	return &JobServiceServer{jobServiceConfig: config, jobRepository: sqlService}
}

func (s *JobServiceServer) GetJobStatus(ctx context.Context, opts *js.JobServiceRequest) (*js.JobServiceResponse, error) {
	g, _ := errgroup.WithContext(ctx)

	if !s.jobRepository.IsJobSetSubscribed(opts.Queue, opts.JobSetId) {

		eventClient := events.NewEventClient(&s.jobServiceConfig.ApiConnection)
		eventJob := eventstojobs.NewEventsToJobService(opts.Queue, opts.JobSetId, opts.JobId, eventClient, s.jobRepository)
		g.Go(func() error {
			return eventJob.SubscribeToJobSetId(context.Background(), s.jobServiceConfig.SubscribeJobSetTime)
		})
	}
	s.jobRepository.UpdateJobSetTime(opts.Queue, opts.JobSetId)
	response, err := s.jobRepository.GetJobStatus(opts.JobId)
	if err != nil {
		log.Warn(err)
		return nil, err
	}

	return response, err
}

func (s *JobServiceServer) Health(ctx context.Context, _ *types.Empty) (*js.HealthCheckResponse, error) {
	eventClient := events.NewEventClient(&s.jobServiceConfig.ApiConnection)
	_, err := eventClient.Health(context.Background(), &types.Empty{})
	if err != nil {
		log.Errorf("Health Check Failed for Events with %s", err)
		return nil, err
	}
	return &js.HealthCheckResponse{Status: js.HealthCheckResponse_SERVING}, nil
}
