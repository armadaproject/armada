package scheduler

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/pkg/errors"
)

type ExecutorApi struct {
	producer             pulsar.Producer
	jobRepository        database.JobRepository
	executorRepository   database.ExecutorRepository
	maxJobsPerCall       int
	maxPulsarMessageSize int
}

func NewExecutorApi(producer pulsar.Producer,
	jobRepository database.JobRepository,
	executorRepository database.ExecutorRepository,
	maxJobsPerCall int,
	maxPulsarMessageSize int) *ExecutorApi {
	return &ExecutorApi{
		producer:             producer,
		jobRepository:        jobRepository,
		executorRepository:   executorRepository,
		maxJobsPerCall:       maxJobsPerCall,
		maxPulsarMessageSize: maxPulsarMessageSize,
	}
}

func (srv *ExecutorApi) LeaseJobRuns(stream executorapi.ExecutorApi_LeaseJobRunsServer) error {
	log := ctxlogrus.Extract(stream.Context())
	// Receive once to get info necessary to get jobs to lease.
	req, err := stream.Recv()
	if err != nil {
		return errors.WithStack(err)
	}

	log.Infof("Handling lease request for executor %s", req.ExecutorId)

	// store the request so that updated usage can be used for scheduling
	err = srv.executorRepository.StoreRequest(req)
	if err != nil {
		return err
	}

	requestRuns := extractRunIds(req)
	log.Debugf("Executor is currently aware of %d job runs", len(requestRuns))

	runsToCancel, err := srv.jobRepository.FindInactiveRuns(stream.Context(), requestRuns)
	if err != nil {
		return err
	}
	log.Debugf("Detected %d runs that need cancelling", len(runsToCancel))

	// Fetch new Run Ids
	runs, err := srv.jobRepository.FetchNewRuns(stream.Context(), req.ExecutorId, srv.maxJobsPerCall, requestRuns)

	if err != nil {
		return err
	}
	decompressor, err := compress.NewZlibDecompressor()
	if err != nil {
		return err
	}

	for i, run := range runs {
		submitMsg := &armadaevents.SubmitJob{}
		err = decompressAndMarshall(run.SubmitMessage, decompressor, submitMsg)
		if err != nil {
			return err
		}

		var groups []string
		if len(run.Groups) > 0 {
			groups, err = compress.DecompressStringArray(run.Groups, decompressor)
			if err != nil {
				return err
			}
		}

		lease := &executorapi.JobRunLease{
			JobRunId: armadaevents.ProtoUuidFromUuid(run.RunID),
			Queue:    run.Queue,
			Jobset:   run.JobSet,
			User:     run.UserID,
			Groups:   groups,
			Job:      submitMsg,
		}
		err = stream.Send(&executorapi.LeaseResponse{
			JobRunIdsToCancel: nil,
			Lease:             lease,
			Final:             i == len(runs)-1,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (srv *ExecutorApi) ReportEvents(ctx context.Context, list *executorapi.EventList) (*types.Empty, error) {
	return nil, srv.publishToPulsar(ctx, list.Events)
}

// PublishToPulsar sends pulsar messages async
func (srv *ExecutorApi) publishToPulsar(ctx context.Context, sequences []*armadaevents.EventSequence) error {
	return pulsarutils.CompactAndPublishSequences(ctx, sequences, srv.producer, srv.maxPulsarMessageSize)
}

func extractRunIds(req *executorapi.LeaseRequest) []uuid.UUID {
	runIds := make([]uuid.UUID, 0)
	// add all runids from nodes
	for _, node := range req.Nodes {
		for _, runId := range node.RunIds {
			runIds = append(runIds, armadaevents.UuidFromProtoUuid(&runId))
		}
	}
	// add all unassigned runids
	for _, runId := range req.UnassignedJobRunIds {
		runIds = append(runIds, armadaevents.UuidFromProtoUuid(&runId))

	}
	return runIds
}

func decompressAndMarshall(b []byte, decompressor compress.Decompressor, msg proto.Message) error {
	decompressed, err := decompressor.Decompress(b)
	if err != nil {
		return err
	}
	return proto.Unmarshal(decompressed, msg)
}
