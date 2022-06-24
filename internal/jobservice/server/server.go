package server

import (
	"context"

	"github.com/G-Research/armada/pkg/api/jobservice"
)

type JobServiceServer struct {
	jobservice.UnimplementedJobServiceServer
}

func NewJobCacheServer() *JobServiceServer {
	return &JobServiceServer{}
} 

func (s *JobServiceServer) GetJobStatus(ctx context.Context, opts *jobservice.JobServiceRequest) (*jobservice.JobServiceResponse, error) {
	return &jobservice.JobServiceResponse{State: "success"}, nil
}
