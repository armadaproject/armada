package scheduler

import (
	"context"
	"github.com/armadaproject/armada/pkg/executorapi"
	"io"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/jackc/pgx/v4/pgxpool"
	pool "github.com/jolestar/go-commons-pool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type ExecutorApi struct {
	api.UnimplementedAggregatedQueueServer
	Producer         pulsar.Producer
	Db               *pgxpool.Pool
	MaxJobsPerCall   int32
	decompressorPool *pool.ObjectPool
}

func (srv *ExecutorApi) LeaseJobRuns(stream executorapi.ExecutorApi_LeaseJobRunsServer) error {
	log := ctxlogrus.Extract(stream.Context())

	// Receive once to get info necessary to get jobs to lease.
	req, err := stream.Recv()
	if err != nil {
		return errors.WithStack(err)
	}

	// TODO: store request

	// extract currently known run ids from request
	// select all active job run id for this executor which aren't in current job runs
	log.Infof("leasing jobs to executor> %+v", runs)

	// Get data stored in sql for these jobs.
	// In particular, the Pulsar submit job message for each job.
	jobIds := make([]string, len(runs))
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

	// The executors expect the legacy job definition.
	// So we need to convert from the Pulsar submit message to a legacy job.
	jobTime := time.Now()
	jobsToLease := make([]*api.Job, len(logJobs))

	decompErr := srv.withDecompressor(func(decompressor compress.Decompressor) error {
		for i, sqlJob := range sqlJobs {
			submitMessage, err := decompressor.Decompress(sqlJob.SubmitMessage)
			if err != nil {
				return err
			}
			logJob := &armadaevents.SubmitJob{}
			err = proto.Unmarshal(submitMessage, logJob)
			if err != nil {
				return errors.WithStack(err)
			}
			logJobs[i] = logJob
		}

		for i, logJob := range logJobs {

			groups, err := compress.DecompressStringArray(sqlJobs[i].Groups, decompressor)
			if err != nil {
				return err
			}

			legacyJob, err := eventutil.ApiJobFromLogSubmitJob(
				sqlJobs[i].UserID,
				groups,
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
		return nil
	})

	if decompErr != nil {
		log.WithError(err).Error("error decompressing")
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
			err := queries.MarkRunsAsSentByExecutorAndJobId(context.Background(), schedulerdb.MarkRunsAsSentByExecutorAndJobIdParams{
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

func (srv *ExecutorApi) ReportEvents(ctx context.Context, list *executorapi.EventList) (*types.Empty, error) {
	//TODO implement me
	panic("implement me")
}

// PublishToPulsar sends pulsar messages async
func (srv *ExecutorApi) publishToPulsar(ctx context.Context, sequences []*armadaevents.EventSequence) error {
	return pulsarutils.CompactAndPublishSequences(ctx, sequences, srv.Producer, 4194304) // 4 MB
}

func (srv *ExecutorApi) withDecompressor(action func(decompressor compress.Decompressor) error) error {
	decompressor, err := srv.decompressorPool.BorrowObject(context.Background())
	if err != nil {
		return errors.WithMessagef(err, "failed to borrow decompressor")
	}
	defer func() {
		err := srv.decompressorPool.ReturnObject(context.Background(), decompressor)
		if err != nil {
			log.
				WithError(errors.WithStack(err)).Warn("error returning decompressor to pool")
		}
	}()

	return action(decompressor.(compress.Decompressor))
}
