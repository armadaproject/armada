package fake

import (
	"context"

	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/pkg/api"
)

type queueClientMock struct {
}

func (queueClientMock) LeaseJobs(ctx context.Context, in *api.LeaseRequest, opts ...grpc.CallOption) (*api.JobLease, error) {
	return &api.JobLease{}, nil
}

func (queueClientMock) RenewLease(ctx context.Context, in *api.RenewLeaseRequest, opts ...grpc.CallOption) (*api.IdList, error) {
	return &api.IdList{}, nil
}

func (queueClientMock) ReturnLease(ctx context.Context, in *api.ReturnLeaseRequest, opts ...grpc.CallOption) (*types.Empty, error) {
	return &types.Empty{}, nil
}

func (queueClientMock) ReportDone(ctx context.Context, in *api.IdList, opts ...grpc.CallOption) (*api.IdList, error) {
	return &api.IdList{}, nil
}
