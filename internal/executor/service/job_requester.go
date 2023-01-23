package service

import (
	"context"
	"io"
	"time"

	"github.com/armadaproject/armada/internal/common"
	clusterContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
	grpcretry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
)

type LeaseRequester interface {
	LeaseJobRuns(availableResource *common.ComputeResources, nodes []*api.NodeInfo, unassignedJobRunIds []armadaevents.Uuid) ([]*executorapi.JobRunLease, []*armadaevents.Uuid, error)
}

type JobLeaseRequester struct {
	executorApiClient executorapi.ExecutorApiClient
	clusterIdentity   clusterContext.ClusterIdentity
	minimumJobSize    common.ComputeResources
}

func NewJobLeaseRequester() *JobLeaseRequester {
	return &JobLeaseRequester{}
}

func (requester *JobLeaseRequester) LeaseJobRuns(
	availableResource *common.ComputeResources,
	nodes []*api.NodeInfo,
	unassignedJobRunIds []armadaevents.Uuid) ([]*executorapi.JobRunLease, []*armadaevents.Uuid, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := requester.executorApiClient.LeaseJobRuns(ctx, grpcretry.Disable(), grpc.UseCompressor(gzip.Name))
	if err != nil {
		return nil, nil, err
	}
	leaseRequest := &executorapi.LeaseRequest{
		ExecutorId:          requester.clusterIdentity.GetClusterId(),
		Pool:                requester.clusterIdentity.GetClusterPool(),
		MinimumJobSize:      requester.minimumJobSize,
		Resources:           *availableResource,
		Nodes:               nodes,
		UnassignedJobRunIds: unassignedJobRunIds,
	}
	if err := stream.Send(leaseRequest); err != nil {
		return nil, nil, errors.WithStack(err)
	}

	jobRuns := make([]*executorapi.JobRunLease, 0, 10)
	jobsToCancel := make([]*armadaevents.Uuid, 0, 10)
	for {
		shouldEndStreamCall := false
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				shouldEndStreamCall = true
			} else {
				return nil, nil, ctx.Err()
			}
		default:
			res, err := stream.Recv()
			if err == io.EOF {
				shouldEndStreamCall = true
			} else if err != nil {
				return nil, nil, err
			}

			switch typed := res.GetEvent().(type) {
			case *executorapi.LeaseStreamMessage_Lease:
				jobRuns = append(jobRuns, typed.Lease)
			case *executorapi.LeaseStreamMessage_CancelRuns:
				jobsToCancel = append(jobsToCancel, typed.CancelRuns.JobRunIdsToCancel...)
			case *executorapi.LeaseStreamMessage_End:
				shouldEndStreamCall = true
			}
		}

		if shouldEndStreamCall {
			break
		}
	}

	return jobRuns, jobsToCancel, nil

}
