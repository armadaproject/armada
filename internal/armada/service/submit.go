package service

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SubmitServer struct {
	JobRepository repository.JobRepository
}

func (server SubmitServer) SubmitJob(ctx context.Context, req *api.JobRequest) (*api.JobSubmitResponse, error) {

	jobId, e := server.JobRepository.AddJob(req)

	if e != nil {
		return nil, status.Errorf(codes.Aborted, e.Error())
	}
	result := &api.JobSubmitResponse{ JobId: jobId }
	return result, nil
}
