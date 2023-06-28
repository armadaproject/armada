package scheduleringester

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
)

// SchedulerDb writes DbOperations into postgres.
type SchedulerDb struct {
	// Connection to the postgres database.
	db             *pgxpool.Pool
	metrics        *metrics.Metrics
	initialBackOff time.Duration
	maxBackOff     time.Duration
	lockTimeout    time.Duration
}

func NewSchedulerDb(
	db *pgxpool.Pool,
	metrics *metrics.Metrics,
	initialBackOff time.Duration,
	maxBackOff time.Duration,
	lockTimeout time.Duration,
) ingest.Sink[*DbOperationsWithMessageIds] {
	return &SchedulerDb{
		db:             db,
		metrics:        metrics,
		initialBackOff: initialBackOff,
		maxBackOff:     maxBackOff,
		lockTimeout:    lockTimeout,
	}
}

// Store persists all operations in the database.
// This function retires until it either succeeds or encounters a terminal error.
// This function locks the postgres table to avoid write conflicts; see acquireLock() for details.
func (s *SchedulerDb) Store(ctx context.Context, instructions *DbOperationsWithMessageIds) error {
	return ingest.WithRetry(func() (bool, error) {
		err := s.db.BeginTxFunc(ctx, pgx.TxOptions{
			IsoLevel:       pgx.ReadCommitted,
			AccessMode:     pgx.ReadWrite,
			DeferrableMode: pgx.Deferrable,
		}, func(tx pgx.Tx) error {
			lockCtx, cancel := context.WithTimeout(ctx, s.lockTimeout)
			defer cancel()
			// The lock is released automatically on transaction rollback/commit.
			if err := s.acquireLock(lockCtx, tx); err != nil {
				return err
			}
			for _, dbOp := range instructions.Ops {
				if err := s.WriteDbOp(ctx, tx, dbOp); err != nil {
					return err
				}
			}
			return nil
		})
		return true, err
	}, s.initialBackOff, s.maxBackOff)
}

// acquireLock acquires a postgres advisory lock, thus preventing concurrent writes.
// This is necessary to ensure sequence numbers assigned to each inserted row are monotonically increasing.
// Such a sequence number is assigned to each inserted row by a postgres function.
//
// Hence, if rows are inserted across multiple transactions concurrently,
// sequence numbers may be interleaved between transactions and the slower transaction may insert
// rows with sequence numbers smaller than those already written.
//
// The scheduler relies on these sequence numbers to only fetch new or updated rows in each update cycle.
func (s *SchedulerDb) acquireLock(ctx context.Context, tx pgx.Tx) error {
	const lockId = 8741339439634283896
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockId); err != nil {
		return errors.Wrapf(err, "could not obtain lock")
	}
	return nil
}

func (s *SchedulerDb) WriteDbOp(ctx context.Context, tx pgx.Tx, op DbOperation) error {
	queries := schedulerdb.New(tx)
	switch o := op.(type) {
	case InsertJobs:
		records := make([]any, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		err := database.Upsert(ctx, tx, "jobs", records)
		if err != nil {
			return err
		}
	case InsertRuns:
		records := make([]any, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v.dbRun
			i++
		}
		err := database.Upsert(ctx, tx, "runs", records)
		if err != nil {
			return err
		}
	case UpdateJobSetPriorities:
		for jobSetInfo, priority := range o {
			err := queries.UpdateJobPriorityByJobSet(
				ctx,
				schedulerdb.UpdateJobPriorityByJobSetParams{
					JobSet:   jobSetInfo.jobSet,
					Queue:    jobSetInfo.queue,
					Priority: priority,
				},
			)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	case UpdateJobSchedulingInfo:
		updateJobInfoSqlStatement := "update jobs set scheduling_info = $1::bytea, scheduling_info_version = $2::int where job_id = $3 and $2::int > scheduling_info_version"

		batch := &pgx.Batch{}
		for key, value := range o {
			batch.Queue(updateJobInfoSqlStatement, value.JobSchedulingInfo, value.JobSchedulingInfoVersion, key)
		}

		err := execBatch(ctx, tx, batch)
		if err != nil {
			return errors.WithStack(err)
		}
	case UpdateJobQueuedState:
		updateQueuedStateSqlStatement := "update jobs set queued = $1::bool, queued_version = $2::int where job_id = $3 and $2::int > queued_version"

		batch := &pgx.Batch{}
		for key, value := range o {
			batch.Queue(updateQueuedStateSqlStatement, value.Queued, value.QueuedStateVersion, key)
		}

		err := execBatch(ctx, tx, batch)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkJobSetsCancelRequested:
		for jobSetInfo, cancelDetails := range o {
			queuedStatesToCancel := make([]bool, 0, 2)
			if cancelDetails.cancelQueued {
				// Cancel all jobs in a queued state
				queuedStatesToCancel = append(queuedStatesToCancel, true)
			}
			if cancelDetails.cancelLeased {
				// Cancel all jobs in a non-queued state
				queuedStatesToCancel = append(queuedStatesToCancel, false)
			}
			err := queries.MarkJobsCancelRequestedBySetAndQueuedState(
				ctx,
				schedulerdb.MarkJobsCancelRequestedBySetAndQueuedStateParams{
					Queue:        jobSetInfo.queue,
					JobSet:       jobSetInfo.jobSet,
					QueuedStates: queuedStatesToCancel,
				},
			)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	case MarkJobsCancelRequested:
		jobIds := maps.Keys(o)
		err := queries.MarkJobsCancelRequestedById(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkJobsCancelled:
		jobIds := maps.Keys(o)
		err := queries.MarkJobsCancelledById(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
		err = queries.MarkRunsCancelledByJobId(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkJobsSucceeded:
		jobIds := maps.Keys(o)
		err := queries.MarkJobsSucceededById(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkJobsFailed:
		jobIds := maps.Keys(o)
		err := queries.MarkJobsFailedById(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case UpdateJobPriorities:
		// TODO: This will be slow if there's a large number of ids.
		// Could be addressed by using a separate table for priority + upsert.
		for jobId, priority := range o {
			err := queries.UpdateJobPriorityById(ctx, schedulerdb.UpdateJobPriorityByIdParams{
				JobID:    jobId,
				Priority: priority,
			})
			if err != nil {
				return errors.WithStack(err)
			}
		}
	case MarkRunsSucceeded:
		runIds := maps.Keys(o)
		err := queries.MarkJobRunsSucceededById(ctx, runIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkRunsFailed:
		runIds := maps.Keys(o)
		returned := make([]uuid.UUID, 0, len(runIds))
		runAttempted := make([]uuid.UUID, 0, len(runIds))
		for k, v := range o {
			if v.LeaseReturned {
				returned = append(returned, k)
			}
			if v.RunAttempted {
				runAttempted = append(runAttempted, k)
			}
		}
		err := queries.MarkJobRunsFailedById(ctx, runIds)
		if err != nil {
			return errors.WithStack(err)
		}
		err = queries.MarkJobRunsReturnedById(ctx, returned)
		if err != nil {
			return errors.WithStack(err)
		}
		err = queries.MarkJobRunsAttemptedById(ctx, runAttempted)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkRunsRunning:
		runIds := maps.Keys(o)
		err := queries.MarkJobRunsRunningById(ctx, runIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case InsertJobRunErrors:
		records := make([]any, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		return database.Upsert(ctx, tx, "job_run_errors", records)
	case *InsertPartitionMarker:
		for _, marker := range o.markers {
			err := queries.InsertMarker(ctx, schedulerdb.InsertMarkerParams{
				GroupID:     marker.GroupID,
				PartitionID: marker.PartitionID,
				Created:     marker.Created,
			})
			if err != nil {
				return errors.Wrapf(err, "error inserting partition marker")
			}
		}
		return nil
	default:
		return errors.Errorf("received unexpected op %+v", op)
	}
	return nil
}

func execBatch(ctx context.Context, tx pgx.Tx, batch *pgx.Batch) error {
	result := tx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		_, err := result.Exec()
		if err != nil {
			return err
		}
	}

	err := result.Close()
	if err != nil {
		return err
	}
	return nil
}
