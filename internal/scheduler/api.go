package scheduler

import (
	"context"
	"strings"

	"github.com/armadaproject/armada/internal/common/schedulers"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

// ExecutorApi is a gRPC service that exposes functionality required by the armada executors
type ExecutorApi struct {
	producer                 pulsar.Producer
	jobRepository            database.JobRepository
	executorRepository       database.ExecutorRepository
	legacyExecutorRepository database.ExecutorRepository
	allowedPriorities        []int32 // allowed priority classes
	maxJobsPerCall           uint    // maximum number of jobs that will be leased in a single call
	maxPulsarMessageSize     uint    // maximum sizer of pulsar messages produced
	clock                    clock.Clock
}

func NewExecutorApi(producer pulsar.Producer,
	jobRepository database.JobRepository,
	executorRepository database.ExecutorRepository,
	legacyExecutorRepository database.ExecutorRepository,
	allowedPriorities []int32,
	maxJobsPerCall uint,
) (*ExecutorApi, error) {
	if len(allowedPriorities) == 0 {
		return nil, errors.New("allowedPriorities cannot be empty")
	}
	if maxJobsPerCall == 0 {
		return nil, errors.New("maxJobsPerCall cannot be 0")
	}

	return &ExecutorApi{
		producer:                 producer,
		jobRepository:            jobRepository,
		executorRepository:       executorRepository,
		legacyExecutorRepository: legacyExecutorRepository,
		allowedPriorities:        allowedPriorities,
		maxJobsPerCall:           maxJobsPerCall,
		maxPulsarMessageSize:     1024 * 1024 * 2,
		clock:                    clock.RealClock{},
	}, nil
}

// LeaseJobRuns performs the following actions:
//   - Stores the request in postgres so that the scheduler can use the job + capacity information in the next scheduling round
//   - Determines if any of the job runs in the request are no longer active and should be cancelled
//   - Determines if any new job runs should be leased to the executor
func (srv *ExecutorApi) LeaseJobRuns(stream executorapi.ExecutorApi_LeaseJobRunsServer) error {
	ctx := stream.Context()
	log := ctxlogrus.Extract(ctx)
	// Receive once to get info necessary to get jobs to lease.
	req, err := stream.Recv()
	if err != nil {
		return errors.WithStack(err)
	}

	log.Infof("Handling lease request for executor %s", req.ExecutorId)

	// store the executor state for use by the scheduler
	executorState := srv.createExecutorState(ctx, req)
	if err = srv.executorRepository.StoreExecutor(stream.Context(), executorState); err != nil {
		return err
	}

	// store the executor state  for the legacy executor to use
	if err = srv.legacyExecutorRepository.StoreExecutor(stream.Context(), executorState); err != nil {
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
	decompressor := compress.NewZlibDecompressor()
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
	err := pulsarutils.CompactAndPublishSequences(ctx, list.Events, srv.producer, srv.maxPulsarMessageSize, schedulers.Pulsar)
	return &types.Empty{}, err
}

// createExecutorState extracts a schedulerobjects.Executor from the requesrt
func (srv *ExecutorApi) createExecutorState(ctx context.Context, req *executorapi.LeaseRequest) *schedulerobjects.Executor {
	log := ctxlogrus.Extract(ctx)
	nodes := make([]*schedulerobjects.Node, 0, len(req.Nodes))
	for _, nodeInfo := range req.Nodes {
		node, err := api.NewNodeFromNodeInfo(nodeInfo, req.ExecutorId, srv.allowedPriorities, srv.clock.Now().UTC())
		if err != nil {
			logging.WithStacktrace(log, err).Warnf(
				"skipping node %s from executor %s", nodeInfo.GetName(), req.GetExecutorId(),
			)
		} else {
			nodes = append(nodes, node)
		}
	}
	return &schedulerobjects.Executor{
		Id:             req.ExecutorId,
		Pool:           req.Pool,
		Nodes:          nodes,
		MinimumJobSize: schedulerobjects.ResourceList{Resources: req.MinimumJobSize},
		LastUpdateTime: srv.clock.Now().UTC(),
		UnassignedJobRuns: slices.Map(req.UnassignedJobRunIds, func(x armadaevents.Uuid) string {
			return strings.ToLower(armadaevents.UuidFromProtoUuid(&x).String())
		}),
	}
}

// extractRunIds extracts all the job runs contained in the executor request
func extractRunIds(req *executorapi.LeaseRequest) ([]uuid.UUID, error) {
	runIds := make([]uuid.UUID, 0)
	// add all runids from nodes
	for _, node := range req.Nodes {
		for runIdStr, _ := range node.RunIdsByState {
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
