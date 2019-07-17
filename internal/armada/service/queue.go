package service

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
)

type AggregatedQueueServer struct {
	JobRepository repository.JobRepository
}

func (AggregatedQueueServer) LeaseJobs(context.Context, *api.LeaseRequest) (*api.JobLease, error) {
	panic("implement me")
}

func (AggregatedQueueServer) RenewLease(context.Context, *api.IdList) (*api.Empty, error) {
	panic("implement me")
}

func (AggregatedQueueServer) ReportDone(context.Context, *api.IdList) (*api.Empty, error) {
	panic("implement me")
}

