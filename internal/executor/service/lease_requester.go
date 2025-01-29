package service

import (
	"fmt"
	"io"

	grpcretry "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	log "github.com/armadaproject/armada/internal/common/logging"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	clusterContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/pkg/executorapi"
)

type LeaseRequest struct {
	AvailableResource   armadaresource.ComputeResources
	Nodes               []*executorapi.NodeInfo
	UnassignedJobRunIds []string
	MaxJobsToLease      uint32
}

type LeaseResponse struct {
	LeasedRuns      []*executorapi.JobRunLease
	RunIdsToCancel  []string
	RunIdsToPreempt []string
}

type LeaseRequester interface {
	LeaseJobRuns(ctx *armadacontext.Context, request *LeaseRequest) (*LeaseResponse, error)
}

type JobLeaseRequester struct {
	executorApiClient executorapi.ExecutorApiClient
	clusterIdentity   clusterContext.ClusterIdentity
}

func NewJobLeaseRequester(
	executorApiClient executorapi.ExecutorApiClient,
	clusterIdentity clusterContext.ClusterIdentity,
) *JobLeaseRequester {
	return &JobLeaseRequester{
		executorApiClient: executorApiClient,
		clusterIdentity:   clusterIdentity,
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
		Resources:           request.AvailableResource.ToProtoMap(),
		Nodes:               request.Nodes,
		UnassignedJobRunIds: request.UnassignedJobRunIds,
		MaxJobsToLease:      request.MaxJobsToLease,
	}
	if err := stream.Send(leaseRequest); err != nil {
		return nil, errors.WithStack(err)
	}

	leaseRuns := []*executorapi.JobRunLease{}
	runIdsToCancel := []string{}
	runIdsToPreempt := []string{}
	shouldEndStream := false
	for !shouldEndStream {
		res, err := stream.Recv()
		if err != nil {
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
			shouldEndStream = true
		default:
			log.Errorf("unexpected lease stream message type %T", typed)
		}
	}

	err = closeStream(stream)
	if err != nil {
		log.Warnf("Failed to close lease jobs stream cleanly - %s", err)
	}

	return &LeaseResponse{
		LeasedRuns:      leaseRuns,
		RunIdsToCancel:  runIdsToCancel,
		RunIdsToPreempt: runIdsToPreempt,
	}, nil
}

// This should be called after our end of stream message has been seen (LeaseStreamMessage_End)
// We call recv one more time and expect an EOF back, indicating the stream is properly closed
func closeStream(stream executorapi.ExecutorApi_LeaseJobRunsClient) error {
	res, err := stream.Recv()
	if err == nil {
		switch typed := res.GetEvent().(type) {
		default:
			return fmt.Errorf("failed closing stream - unexpectedly received event of type %T", typed)
		}
	} else if err == io.EOF {
		return nil
	} else {
		return err
	}
}
