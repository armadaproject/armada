package server

import (
	"context"

	"github.com/G-Research/armada/pkg/api/jobcache"
)

type JobCacheServer struct {
	jobcache.UnimplementedJobCacheServer
}

func NewJobCacheServer() *JobCacheServer {
	return &JobCacheServer{}
} 

func (s *JobCacheServer) GetJobStatus(ctx context.Context, opts *jobcache.JobCacheRequest) (*jobcache.JobCacheResponse, error) {
	return &jobcache.JobCacheResponse{State: "success"}, nil
}
