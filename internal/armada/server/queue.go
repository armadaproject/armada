package server

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"github.com/gogo/protobuf/types"
)

type AggregatedQueueServer struct {
	JobRepository repository.JobRepository
	UsageRepository repository.UsageRepository
}

func (AggregatedQueueServer) LeaseJobs(context.Context, *api.LeaseRequest) (*api.JobLease, error) {



	


}

func (AggregatedQueueServer) RenewLease(context.Context, *api.IdList) (*types.Empty, error) {
	panic("implement me")
}

func (AggregatedQueueServer) ReportDone(context.Context, *api.IdList) (*types.Empty, error) {
	panic("implement me")
}
