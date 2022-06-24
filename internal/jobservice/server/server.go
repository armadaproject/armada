package server

import (
	"context"

	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/internal/jobservice/eventstojobs"
	"github.com/G-Research/armada/pkg/api/jobservice"
)

type JobServiceServer struct {
	jobServiceConfig *configuration.JobServiceConfiguration
}

func NewJobService(config *configuration.JobServiceConfiguration) *JobServiceServer {
	return &JobServiceServer{jobServiceConfig: config}
}

func (s *JobServiceServer) GetJobStatus(ctx context.Context, opts *jobservice.JobServiceRequest) (*jobservice.JobServiceResponse, error) {
	eventsToJobService := eventstojobs.NewEventsToJobService(opts.Queue, opts.JobSetId, opts.JobId, s.jobServiceConfig.ApiConnection)
	return eventsToJobService.GetJobStatusUsingEventApi(ctx)
}
