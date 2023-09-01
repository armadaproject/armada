package service

import (
	gocontext "context"
	"io"

	grpcretry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	clusterContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

type LeaseRequest struct {
	AvailableResource   armadaresource.ComputeResources
	Nodes               []*api.NodeInfo
	UnassignedJobRunIds []armadaevents.Uuid
	MaxJobsToLease      uint32
}

type LeaseResponse struct {
	LeasedRuns      []*executorapi.JobRunLease
	RunIdsToCancel  []*armadaevents.Uuid
	RunIdsToPreempt []*armadaevents.Uuid
}

type LeaseRequester interface {
	LeaseJobRuns(ctx *armadacontext.Context, request *LeaseRequest) (*LeaseResponse, error)
}

type JobLeaseRequester struct {
	executorApiClient executorapi.ExecutorApiClient
	clusterIdentity   clusterContext.ClusterIdentity
	minimumJobSize    armadaresource.ComputeResources
}

func NewJobLeaseRequester(
	executorApiClient executorapi.ExecutorApiClient,
	clusterIdentity clusterContext.ClusterIdentity,
	minimumJobSize armadaresource.ComputeResources,
) *JobLeaseRequester {
	return &JobLeaseRequester{
		executorApiClient: executorApiClient,
		clusterIdentity:   clusterIdentity,
		minimumJobSize:    minimumJobSize,
	}
}

func (requester *JobLeaseRequester) LeaseJobRuns(ctx *armadacontext.Context, request *LeaseRequest) (*LeaseResponse, error) {
	stream, err := requester.executorApiClient.LeaseJobRuns(ctx, grpcretry.Disable(), grpc.UseCompressor(gzip.Name))
	if err != nil {
		return nil, err
	}
	leaseRequest := &executorapi.LeaseRequest{
		ExecutorId:          requester.clusterIdentity.GetClusterId(),
		Pool:                requester.clusterIdentity.GetClusterPool(),
		MinimumJobSize:      requester.minimumJobSize,
		Resources:           request.AvailableResource,
		Nodes:               request.Nodes,
		UnassignedJobRunIds: request.UnassignedJobRunIds,
		MaxJobsToLease:      request.MaxJobsToLease,
	}
	if err := stream.Send(leaseRequest); err != nil {
		return nil, errors.WithStack(err)
	}

	leaseRuns := []*executorapi.JobRunLease{}
	runIdsToCancel := []*armadaevents.Uuid{}
	runIdsToPreempt := []*armadaevents.Uuid{}
	for {
		shouldEndStreamCall := false
		select {
		case <-ctx.Done():
			if ctx.Err() == gocontext.DeadlineExceeded {
				shouldEndStreamCall = true
			} else {
				return nil, ctx.Err()
			}
		default:
			res, err := stream.Recv()
			if err == io.EOF {
				shouldEndStreamCall = true
			} else if err != nil {
				return nil, err
			}

			switch typed := res.GetEvent().(type) {
			case *executorapi.LeaseStreamMessage_Lease:
				leaseRuns = append(leaseRuns, typed.Lease)
			case *executorapi.LeaseStreamMessage_PreemptRuns:
				runIdsToPreempt = append(runIdsToPreempt, typed.PreemptRuns.JobRunIdsToPreempt...)
			case *executorapi.LeaseStreamMessage_CancelRuns:
				runIdsToCancel = append(runIdsToCancel, typed.CancelRuns.JobRunIdsToCancel...)
			case *executorapi.LeaseStreamMessage_End:
				shouldEndStreamCall = true
			default:
				log.Errorf("unexpected lease stream message type %T", typed)
			}
		}

		if shouldEndStreamCall {
			break
		}
	}

	return &LeaseResponse{
		LeasedRuns:      leaseRuns,
		RunIdsToCancel:  runIdsToCancel,
		RunIdsToPreempt: runIdsToPreempt,
	}, nil
}
