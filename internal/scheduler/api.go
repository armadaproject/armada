package scheduler

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/armada/server"
	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
)

type ExecutorApi struct {
	api.UnimplementedAggregatedQueueServer
	// Embed the Redis-backed event server.
	// Provides methods for dual-publishing events etc.
	*server.EventServer
	Producer       pulsar.Producer
	Db             *pgxpool.Pool
	MaxJobsPerCall int32
}

func (srv *ExecutorApi) StreamingLeaseJobs(stream api.AggregatedQueue_StreamingLeaseJobsServer) error {
	log := ctxlogrus.Extract(stream.Context())

	// Receive once to get info necessary to get jobs to lease.
	req, err := stream.Recv()
	if err != nil {
		return errors.WithStack(err)
	}

	// Lease requests include the current resource utilisation for all nodes managed by this executor.
	// We write this data into postgres to make it available to the scheduler.
	err = srv.writeNodeInfoToPostgres(stream.Context(), req.ClusterId, req.Nodes)
	if err != nil {
		return err
	}

	// Get leases assigned to this executor.
	queries := New(srv.Db)
	runs, err := queries.SelectNewRunsForExecutorWithLimit(
		stream.Context(),
		SelectNewRunsForExecutorWithLimitParams{
			Executor: req.GetClusterId(),
			Limit:    srv.MaxJobsPerCall,
		},
	)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Infof("leasing jobs to executor> %+v", runs)

	// Get data stored in sql for these jobs.
	// In particular, the Pulsar submit job message for each job.
	jobIds := make([]uuid.UUID, len(runs))
	for i, run := range runs {
		jobIds[i] = run.JobID
	}
	sqlJobs, err := queries.SelectJobsFromIds(stream.Context(), jobIds)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(sqlJobs) != len(runs) {
		err := errors.Errorf("expected %d jobs, but only got %d", len(runs), len(sqlJobs))
		logging.WithStacktrace(log, err).Warn("jobs missing from postgres")
	}

	// Unmarshal the submit job messages.
	// We need these to convert to a form the executor understands.
	logJobs := make([]*armadaevents.SubmitJob, len(sqlJobs))
	for i, sqlJob := range sqlJobs {
		logJob := &armadaevents.SubmitJob{}
		err := proto.Unmarshal(sqlJob.SubmitMessage, logJob)
		if err != nil {
			return errors.WithStack(err)
		}
		logJobs[i] = logJob
	}

	// The executors expect the legacy job definition.
	// So we need to convert from the Pulsar submit message to a legacy job.
	jobTime := time.Now()
	jobsToLease := make([]*api.Job, len(logJobs))
	for i, logJob := range logJobs {
		legacyJob, err := eventutil.ApiJobFromLogSubmitJob(
			sqlJobs[i].UserID,
			sqlJobs[i].Groups,
			sqlJobs[i].Queue,
			sqlJobs[i].JobSet,
			jobTime,
			logJob,
		)
		if err != nil {
			return err
		}
		jobsToLease[i] = legacyJob
	}

	// The server streams jobs to the executor.
	// The executor streams back an ack for each received job.
	// With each job sent to the executor, the server includes the number of received acks.
	//
	// When the connection breaks, the server expires all leases for which it hasn't received an ack
	// and the executor expires all leases for which it hasn't received confirmation that the server received the ack.
	//
	// We track the total number of jobs and the number of jobs for which acks have been received.
	// Because gRPC streams guarantee ordering, we only need to track the number of acks.
	// The client is responsible for acking jobs in the order they are received.
	numJobs := uint32(len(jobsToLease))
	var numAcked uint32

	// Stream the jobs to the executor.
	g, _ := errgroup.WithContext(stream.Context())
	g.Go(func() error {
		for _, job := range jobsToLease {
			err := stream.Send(&api.StreamingJobLease{
				Job:      job,
				NumJobs:  numJobs,
				NumAcked: atomic.LoadUint32(&numAcked),
			})
			if err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
		}
		return nil
	})

	// Listen for job ids being streamed back as they're received.
	// Defer marking all acked as sent in postgres.
	ackedJobIds := make([]uuid.UUID, 0, numJobs)
	defer func() {
		if len(ackedJobIds) > 0 {
			// Use the background context to run even if the stream context is cancelled.
			err := queries.MarkRunsAsSentByExecutorAndJobId(context.Background(), MarkRunsAsSentByExecutorAndJobIdParams{
				Executor: req.GetClusterId(),
				JobIds:   ackedJobIds,
			})
			if err != nil {
				err = errors.WithStack(err)
				logging.WithStacktrace(log, err).Error("failed to mark runs as sent in postgres")
			}
		}
	}()
	g.Go(func() error {
		numJobs := numJobs // Assign a local variable to guarantee there are no race conditions.
		for atomic.LoadUint32(&numAcked) < numJobs {
			ack, err := stream.Recv()
			if err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
			atomic.AddUint32(&numAcked, uint32(len(ack.ReceivedJobIds)))
			for _, s := range ack.ReceivedJobIds {
				protoUuid, err := armadaevents.ProtoUuidFromUlidString(s)
				if err != nil {
					return errors.WithStack(err)
				}
				jobId := armadaevents.UuidFromProtoUuid(protoUuid)
				ackedJobIds = append(ackedJobIds, jobId) // Mark job as sent.
			}
		}
		return nil
	})

	// Wait for all jobs to have been sent and all acks to have been received.
	err = g.Wait()
	if err != nil {
		log.WithError(err).Error("error sending/receiving job leases to/from executor")
	}

	// Send one more message with the total number of acks.
	err = stream.Send(&api.StreamingJobLease{
		Job:      nil, // Omitted
		NumJobs:  numJobs,
		NumAcked: numAcked,
	})
	if err != nil {
		log.WithError(err).Error("error sending the number of acks")
	}

	return nil
}

// writeNodeInfoToPostgres writes the NodeInfo messages received from an executor into postgres
// with the name of the node set as the primary key, i.e., the node name must be unique across all clusters.
func (srv *ExecutorApi) writeNodeInfoToPostgres(ctx context.Context, executorName string, nodeInfos []api.NodeInfo) error {
	records := make([]interface{}, 0)
	for _, nodeInfo := range nodeInfos {
		message, err := proto.Marshal(&nodeInfo)
		if err != nil {
			return errors.WithStack(err)
		}
		records = append(records, Nodeinfo{
			ExecutorNodeName: fmt.Sprintf("%s-%s", executorName, nodeInfo.GetName()),
			NodeName:         nodeInfo.GetName(),
			Executor:         executorName,
			Message:          message,
		})
	}
	return Upsert(ctx, srv.Db, "nodeinfo", NodeInfoSchema(), records)
}

func (srv *ExecutorApi) RenewLease(ctx context.Context, req *api.RenewLeaseRequest) (*api.IdList, error) {
	log := ctxlogrus.Extract(ctx)
	log.Infof("executor %s renewed jobs %v", req.ClusterId, req.Ids)

	if len(req.Ids) == 0 {
		return &api.IdList{
			Ids: make([]string, 0),
		}, nil
	}

	jobIds := make([]uuid.UUID, len(req.Ids))
	for i, s := range req.Ids {
		protoUuid, err := armadaevents.ProtoUuidFromUlidString(s)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		jobIds[i] = armadaevents.UuidFromProtoUuid(protoUuid)
	}

	queries := New(srv.Db)
	runs, err := queries.SelectRunsFromExecutorAndJobs(ctx, SelectRunsFromExecutorAndJobsParams{
		Executor: req.GetClusterId(),
		JobIds:   jobIds,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	responseIds := make([]string, 0, len(runs))
	for _, run := range runs {
		if !run.Cancelled {
			protoUuid := armadaevents.ProtoUuidFromUuid(run.JobID)
			responseId, err := armadaevents.UlidStringFromProtoUuid(protoUuid)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			responseIds = append(responseIds, responseId)
		}
	}

	// TODO: Track when leases are renewed so the scheduler knows when they've been lost.

	return &api.IdList{
		Ids: responseIds,
	}, nil
}

func (srv *ExecutorApi) ReturnLease(ctx context.Context, req *api.ReturnLeaseRequest) (*types.Empty, error) {
	log := ctxlogrus.Extract(ctx)
	log.Infof("executor %s returned %s", req.ClusterId, req.JobId)

	queries := New(srv.Db)

	protoUuid, err := armadaevents.ProtoUuidFromUlidString(req.JobId)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	jobId := armadaevents.UuidFromProtoUuid(protoUuid)

	row, err := queries.SelectQueueJobSetFromId(ctx, jobId)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	runs, err := queries.SelectRunsFromExecutorAndJobs(ctx, SelectRunsFromExecutorAndJobsParams{
		Executor: req.GetClusterId(),
		JobIds:   []uuid.UUID{jobId},
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Return all leases for this job associated with this executor.
	// Needed to work around the fact that executors have no concept of job runs.
	sequence := &armadaevents.EventSequence{
		Queue:      row.Queue,
		JobSetName: row.JobSet,
	}
	for _, run := range runs {
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_JobRunErrors{
				JobRunErrors: &armadaevents.JobRunErrors{
					RunId: armadaevents.ProtoUuidFromUuid(run.RunID),
					JobId: armadaevents.ProtoUuidFromUuid(jobId),
					Errors: []*armadaevents.Error{
						{
							Terminal: true, // EventMessage_LeaseReturned indicates a pod could not be scheduled.
							Reason: &armadaevents.Error_PodLeaseReturned{
								PodLeaseReturned: &armadaevents.PodLeaseReturned{
									ObjectMeta: &armadaevents.ObjectMeta{
										ExecutorId:   req.ClusterId,
										KubernetesId: "", // TODO: The fields explicitly set empty here should be set, but are not available in req.
									},
									PodNumber: 0,
									Message:   "",
								},
							},
						},
					},
				},
			},
		})
	}

	err = srv.publishToPulsar(ctx, []*armadaevents.EventSequence{sequence})
	if err != nil {
		return nil, err
	}

	return &types.Empty{}, nil
}

func (srv *ExecutorApi) ReportDone(ctx context.Context, req *api.IdList) (*api.IdList, error) {
	log := ctxlogrus.Extract(ctx)
	log.Infof("jobs %v reported done", req.Ids)

	queries := New(srv.Db)

	jobIds := make([]uuid.UUID, len(req.Ids))
	for i, s := range req.Ids {
		protoUuid, err := armadaevents.ProtoUuidFromUlidString(s)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		jobId := armadaevents.UuidFromProtoUuid(protoUuid)

		if err != nil {
			return nil, errors.WithStack(err)
		}
		jobIds[i] = jobId
	}

	rows, err := queries.SelectQueueJobSetFromIds(ctx, jobIds)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	sequences := make([]*armadaevents.EventSequence, 0)
	for _, row := range rows {
		sequence := &armadaevents.EventSequence{
			Queue:      row.Queue,
			JobSetName: row.JobSet,
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
				JobRunSucceeded: &armadaevents.JobRunSucceeded{
					RunId: armadaevents.ProtoUuidFromUuid(row.JobID), // TODO: Need at least the executor name to get this.
					JobId: armadaevents.ProtoUuidFromUuid(row.JobID),
				},
			},
		})
	}

	err = srv.publishToPulsar(ctx, sequences)
	if err != nil {
		return nil, err
	}

	return &api.IdList{
		Ids: req.Ids,
	}, nil
}

// TODO: Does nothing for now.
func (srv *ExecutorApi) ReportUsage(ctx context.Context, req *api.ClusterUsageReport) (*types.Empty, error) {
	return &types.Empty{}, nil
}

// PublishToPulsar sends pulsar messages async
func (srv *ExecutorApi) publishToPulsar(ctx context.Context, sequences []*armadaevents.EventSequence) error {
	return pulsarutils.CompactAndPublishSequences(ctx, sequences, srv.Producer, 4194304) // 4 MB
}
