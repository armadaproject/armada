package server

import (
	"context"

	"github.com/gogo/protobuf/types"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/jobservice/configuration"
	"github.com/armadaproject/armada/internal/jobservice/events"
	"github.com/armadaproject/armada/internal/jobservice/repository"

	js "github.com/armadaproject/armada/pkg/api/jobservice"
)

type JobServiceServer struct {
	jobServiceConfig *configuration.JobServiceConfiguration
	jobRepository    *repository.SQLJobService
}

func NewJobService(config *configuration.JobServiceConfiguration, sqlService *repository.SQLJobService) *JobServiceServer {
	return &JobServiceServer{jobServiceConfig: config, jobRepository: sqlService}
}

func (s *JobServiceServer) GetJobStatus(ctx context.Context, opts *js.JobServiceRequest) (*js.JobServiceResponse, error) {
	requestFields := log.Fields{
		"job_id":     opts.JobId,
		"job_set_id": opts.JobSetId,
		"queue":      opts.Queue,
	}

	jobSetExists, err := s.jobRepository.IsJobSetSubscribed(opts.Queue, opts.JobSetId)
	if err != nil {
		log.Error("Error checking if job is subscribed", err)
	}
	if !jobSetExists {
		errsubscribe := s.jobRepository.SubscribeJobSet(opts.Queue, opts.JobSetId)
		if errsubscribe != nil {
			log.Error("unable to subscribe job set", err)
		}
		log.Infof("Subscribing %s-%s", opts.Queue, opts.JobSetId)
	}
	if err := s.jobRepository.UpdateJobSetDb(opts.Queue, opts.JobSetId); err != nil {
		log.WithFields(requestFields).Warn(err)
	}
	response, err := s.jobRepository.GetJobStatus(opts.JobId)
	if err != nil {
		log.WithFields(requestFields).Error(err)
		return nil, err
	}
	if response.State == js.JobServiceResponse_SUCCEEDED {
		log.WithFields(requestFields).Info("job succeeded")
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
