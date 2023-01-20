package scheduler

import (
	"context"
	"time"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

// ExecutorApi is a gRPC service that exposes functionality required by the armada executors
type ExecutorApi struct {
	producer             pulsar.Producer
	jobRepository        database.JobRepository
	executorRepository   database.ExecutorRepository
	maxJobsPerCall       int // maximum number of jobs that will be leased in a single call
	maxPulsarMessageSize int // maximum sizer of pulsar messages produced
	allowedPriorities    []int32
}

func NewExecutorApi(producer pulsar.Producer,
	jobRepository database.JobRepository,
	executorRepository database.ExecutorRepository,
	maxJobsPerCall int,
	maxPulsarMessageSize int,
) *ExecutorApi {
	return &ExecutorApi{
		producer:             producer,
		jobRepository:        jobRepository,
		executorRepository:   executorRepository,
		maxJobsPerCall:       maxJobsPerCall,
		maxPulsarMessageSize: maxPulsarMessageSize,
	}
}

// LeaseJobRuns performs the following actions:
//   - Stores the request in postgres so that the scheduler can use the job + capacity information in the next scheduling round
//   - Determines if any of the job runs in the request are no longer active and should be cancelled
//   - Determines if any new job runs should be leased to the executor
func (srv *ExecutorApi) LeaseJobRuns(stream executorapi.ExecutorApi_LeaseJobRunsServer) error {
	log := ctxlogrus.Extract(stream.Context())
	// Receive once to get info necessary to get jobs to lease.
	req, err := stream.Recv()
	if err != nil {
		return errors.WithStack(err)
	}

	log.Infof("Handling lease request for executor %s", req.ExecutorId)

	// store the executor state for use by the scheduler
	executorState := createExecutorState(req, srv.allowedPriorities)
	err = srv.executorRepository.StoreExecutor(stream.Context(), executorState)
	if err != nil {
		return err
	}

	requestRuns, err := extractRunIds(req)
	if err != nil {
		return err
	}
	log.Debugf("Executor is currently aware of %d job runs", len(requestRuns))

	runsToCancel, err := srv.jobRepository.FindInactiveRuns(stream.Context(), requestRuns)
	if err != nil {
		return err
	}
	log.Debugf("Detected %d runs that need cancelling", len(runsToCancel))

	// Fetch new leases from the db
	leases, err := srv.jobRepository.FetchJobRunLeases(stream.Context(), req.ExecutorId, srv.maxJobsPerCall, requestRuns)
	if err != nil {
		return err
	}
	decompressor, err := compress.NewZlibDecompressor()
	if err != nil {
		return err
	}

	// if necessary send a list of runs to cancel
	if len(runsToCancel) > 0 {
		err = stream.Send(&executorapi.LeaseStreamMessage{
			Event: &executorapi.LeaseStreamMessage_CancelRuns{
				CancelRuns: &executorapi.CancelRuns{
					JobRunIdsToCancel: slices.Map(runsToCancel, func(x uuid.UUID) *armadaevents.Uuid {
						return armadaevents.ProtoUuidFromUuid(x)
					}),
				},
			},
		})

		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Now send any leases
	for _, lease := range leases {
		submitMsg := &armadaevents.SubmitJob{}
		err = decompressAndMarshall(lease.SubmitMessage, decompressor, submitMsg)
		if err != nil {
			return err
		}

		var groups []string
		if len(lease.Groups) > 0 {
			groups, err = compress.DecompressStringArray(lease.Groups, decompressor)
			if err != nil {
				return err
			}
		}
		err = stream.Send(&executorapi.LeaseStreamMessage{
			Event: &executorapi.LeaseStreamMessage_Lease{
				Lease: &executorapi.JobRunLease{
					JobRunId: armadaevents.ProtoUuidFromUuid(lease.RunID),
					Queue:    lease.Queue,
					Jobset:   lease.JobSet,
					User:     lease.UserID,
					Groups:   groups,
					Job:      submitMsg,
				},
			},
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Finally, send an end marker
	err = stream.Send(&executorapi.LeaseStreamMessage{
		Event: &executorapi.LeaseStreamMessage_End{
			End: &executorapi.EndMarker{},
		},
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// ReportEvents publishes all events to pulsar. The events are compacted for more efficient publishing
func (srv *ExecutorApi) ReportEvents(ctx context.Context, list *executorapi.EventList) (*types.Empty, error) {
	err := pulsarutils.CompactAndPublishSequences(ctx, list.Events, srv.producer, srv.maxPulsarMessageSize)
	return &types.Empty{}, err
}

// extractRunIds extracts all the job runs contained in the executor request
func extractRunIds(req *executorapi.LeaseRequest) ([]uuid.UUID, error) {
	runIds := make([]uuid.UUID, 0)
	// add all runids from nodes
	for _, node := range req.Nodes {
		for _, runIdStr := range node.RunIds {
			runId, err := uuid.Parse(runIdStr)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			runIds = append(runIds, runId)
		}
	}
	// add all unassigned runidsreq *executorapi.LeaseRequest
	for _, runId := range req.UnassignedJobRunIds {
		runIds = append(runIds, armadaevents.UuidFromProtoUuid(&runId))
	}
	return runIds, nil
}

func decompressAndMarshall(b []byte, decompressor compress.Decompressor, msg proto.Message) error {
	decompressed, err := decompressor.Decompress(b)
	if err != nil {
		return err
	}
	return proto.Unmarshal(decompressed, msg)
}

func createExecutorState(req *executorapi.LeaseRequest, allowedPriorities []int32) *schedulerobjects.Executor {
	nodes := make([]*schedulerobjects.Node, len(req.Nodes))
	for i, nodeInfo := range req.Nodes {
		nodes[i] = schedulerobjects.NewNodeFromNodeInfo(nodeInfo, req.ExecutorId, allowedPriorities)
	}
	return &schedulerobjects.Executor{
		Id:             req.ExecutorId,
		Pool:           req.Pool,
		Nodes:          nodes,
		MinimumJobSize: schedulerobjects.ResourceList{Resources: req.MinimumJobSize},
		LastUpdateTime: time.Now(),
	}
}
