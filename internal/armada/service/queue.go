package service

import (
	"context"
	"fmt"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"github.com/gogo/protobuf/types"
)

type AggregatedQueueServer struct {
	JobRepository repository.JobRepository
}

func (AggregatedQueueServer) LeaseJobs(context.Context, *api.LeaseRequest) (*api.JobLease, error) {
	//TODO Implement me
	fmt.Println("Lease jobs called")
	jobLease := api.JobLease{
		Job: make([]*api.Job, 0),
	}

	return &jobLease, nil
}

func (AggregatedQueueServer) RenewLease(context.Context, *api.IdList) (*types.Empty, error) {
	//TODO Implement me
	fmt.Println("Renew lease called")
	return &types.Empty{}, nil
}

func (AggregatedQueueServer) ReportDone(context.Context, *api.IdList) (*types.Empty, error) {
	//TODO Implement me
	fmt.Println("Report done called")
	return &types.Empty{}, nil
}
