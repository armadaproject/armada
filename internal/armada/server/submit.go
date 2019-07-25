package server

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SubmitServer struct {
	jobRepository   repository.JobRepository
	queueRepository repository.QueueRepository
}

func NewSubmitServer(jobRepository repository.JobRepository, queueRepository repository.QueueRepository) *SubmitServer {
	return &SubmitServer{jobRepository: jobRepository, queueRepository: queueRepository}
}

func (server SubmitServer) CreateQueue(ctx context.Context, queue *api.Queue) (*types.Empty, error) {

	e := server.queueRepository.CreateQueue(queue)
	if e != nil {
		return nil, status.Errorf(codes.Aborted, e.Error())
	}
	return &types.Empty{}, nil
}

func (server SubmitServer) SubmitJob(ctx context.Context, req *api.JobRequest) (*api.JobSubmitResponse, error) {

	jobId, e := server.jobRepository.AddJob(req)

	if e != nil {
		return nil, status.Errorf(codes.Aborted, e.Error())
	}
	result := &api.JobSubmitResponse{ JobId: jobId }
	return result, nil
}
