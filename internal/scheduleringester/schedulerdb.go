package scheduleringester

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/tracing"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/controlplaneevents"
)

const (
	InvalidLockKey      = -1
	JobSetEventsLockKey = 8741339439634283896
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
) *SchedulerDb {
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
func (s *SchedulerDb) Store(ctx *armadacontext.Context, instructions *DbOperationsWithMessageIds) error {
	_, span := tracing.StartSpan(ctx, "scheduler-ingester", "store-operations")
	defer span.End()

	span.SetAttributes(
		attribute.Int("db.operations.count", len(instructions.Ops)),
		attribute.Int("db.messages.count", len(instructions.MessageIds)),
	)
	span.SetAttributes(tracing.DatabaseAttributes("batch-store")...)

	err := ingest.WithRetry(func() (bool, error) {
		err := pgx.BeginTxFunc(ctx, s.db, pgx.TxOptions{
			IsoLevel:       pgx.ReadCommitted,
			AccessMode:     pgx.ReadWrite,
			DeferrableMode: pgx.Deferrable,
		}, func(tx pgx.Tx) error {
			lockCtx, cancel := armadacontext.WithTimeout(ctx, s.lockTimeout)
			defer cancel()
			if scope, err := getLockKey(instructions.Ops); err == nil {
				// The lock is released automatically on transaction rollback/commit.
				if err := s.acquireLock(lockCtx, tx, scope); err != nil {
					return err
				}
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

	if err != nil {
		tracing.AddErrorToSpan(span, err)
	} else {
		tracing.AddSuccessToSpan(span)
	}
	return err
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
func (s *SchedulerDb) acquireLock(ctx *armadacontext.Context, tx pgx.Tx, scope int) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", scope); err != nil {
		return errors.Wrapf(err, "could not obtain lock")
	}
	return nil
}

// extractBusinessContextFromOperation extracts business context from database operations
func extractBusinessContextFromOperation(op DbOperation) (queues []string, jobsets []string, jobIds []string, operationType string) {
	switch o := op.(type) {
	case InsertJobs:
		for jobId := range o {
			jobIds = append(jobIds, jobId)
		}
		operationType = "insert_jobs"
	case InsertRuns:
		for jobId := range o {
			jobIds = append(jobIds, jobId)
		}
		operationType = "insert_runs"
	case UpdateJobSetPriorities:
		for jobSetKey := range o {
			queues = append(queues, jobSetKey.queue)
			jobsets = append(jobsets, jobSetKey.jobSet)
		}
		operationType = "update_jobset_priorities"
	case MarkJobSetsCancelRequested:
		for jobSetKey := range o.jobSets {
			queues = append(queues, jobSetKey.queue)
			jobsets = append(jobsets, jobSetKey.jobSet)
		}
		operationType = "mark_jobsets_cancel_requested"
	case MarkJobsCancelRequested:
		for jobSetKey, jobIdList := range o.jobIds {
			queues = append(queues, jobSetKey.queue)
			jobsets = append(jobsets, jobSetKey.jobSet)
			jobIds = append(jobIds, jobIdList...)
		}
		operationType = "mark_jobs_cancel_requested"
	case MarkRunsForJobPreemptRequested:
		for jobSetKey, jobIdList := range o {
			queues = append(queues, jobSetKey.queue)
			jobsets = append(jobsets, jobSetKey.jobSet)
			jobIds = append(jobIds, jobIdList...)
		}
		operationType = "mark_runs_preempt_requested"
	default:
		operationType = fmt.Sprintf("operation_%T", op)
	}
	return queues, jobsets, jobIds, operationType
}

func (s *SchedulerDb) WriteDbOp(ctx *armadacontext.Context, tx pgx.Tx, op DbOperation) error {
	_, span := tracing.StartSpan(ctx, "scheduler-ingester", "write-db-operation")
	defer span.End()

	// Extract business context from the operation
	queues, jobsets, jobIds, operationType := extractBusinessContextFromOperation(op)

	span.SetAttributes(
		attribute.String("db.operation.type", operationType),
		attribute.StringSlice("armada.queues", queues),
		attribute.StringSlice("armada.jobsets", jobsets),
		attribute.StringSlice("armada.job_ids", jobIds),
		attribute.Int("armada.queues.count", len(queues)),
		attribute.Int("armada.jobsets.count", len(jobsets)),
		attribute.Int("armada.job_ids.count", len(jobIds)),
	)

	// Add job metadata if we have queue and jobset information
	if len(queues) > 0 && len(jobsets) > 0 {
		metadata := tracing.JobMetadata{
			Queue:     queues[0],  // Use first queue for correlation
			JobSet:    jobsets[0], // Use first jobset for correlation
			Operation: operationType,
		}
		tracing.AddJobMetadata(span, metadata)
	}

	queries := schedulerdb.New(tx)
	switch o := op.(type) {
	case InsertJobs:
		span.SetAttributes(
			attribute.String("db.table", "jobs"),
			attribute.Int("db.records.count", len(o)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("INSERT")...)

		records := make([]any, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		dbCtx, dbSpan := tracing.StartSpan(ctx.Context, "scheduler-ingester", "db-insert-jobs")
		dbSpan.SetAttributes(
			attribute.String("db.operation", "INSERT"),
			attribute.String("db.table", "jobs"),
			attribute.Int("db.records.count", len(records)),
		)
		dbArmadaCtx := &armadacontext.Context{Context: dbCtx}
		err := database.Upsert(dbArmadaCtx, tx, "jobs", records)
		if err != nil {
			tracing.AddErrorToSpan(span, err)
			tracing.AddErrorToSpan(dbSpan, err)
			dbSpan.End()
			return err
		}
		tracing.AddSuccessToSpan(dbSpan)
		dbSpan.End()
		tracing.AddSuccessToSpan(span)
	case InsertRuns:
		span.SetAttributes(
			attribute.String("db.table", "runs"),
			attribute.Int("db.records.count", len(o)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("INSERT")...)

		records := make([]any, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v.DbRun
			i++
		}
		dbCtx, dbSpan := tracing.StartSpan(ctx.Context, "scheduler-ingester", "db-insert-runs")
		dbSpan.SetAttributes(
			attribute.String("db.operation", "INSERT"),
			attribute.String("db.table", "runs"),
			attribute.Int("db.records.count", len(records)),
		)
		dbArmadaCtx := &armadacontext.Context{Context: dbCtx}
		err := database.Upsert(dbArmadaCtx, tx, "runs", records)
		if err != nil {
			tracing.AddErrorToSpan(span, err)
			tracing.AddErrorToSpan(dbSpan, err)
			dbSpan.End()
			return err
		}
		tracing.AddSuccessToSpan(dbSpan)
		dbSpan.End()
		tracing.AddSuccessToSpan(span)
	case UpdateJobSetPriorities:
		span.SetAttributes(
			attribute.String("db.table", "jobs"),
			attribute.Int("db.updates.count", len(o)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("UPDATE")...)

		for jobSetInfo, priority := range o {
			dbCtx, dbSpan := tracing.StartSpan(ctx.Context, "scheduler-ingester", "db-update-jobset-priority")
			dbSpan.SetAttributes(
				attribute.String("db.operation", "UPDATE"),
				attribute.String("db.table", "jobs"),
				attribute.String("armada.queue", jobSetInfo.queue),
				attribute.String("armada.jobset", jobSetInfo.jobSet),
				attribute.Int64("armada.priority.new", priority),
			)
			err := queries.UpdateJobPriorityByJobSet(
				dbCtx,
				schedulerdb.UpdateJobPriorityByJobSetParams{
					JobSet:   jobSetInfo.jobSet,
					Queue:    jobSetInfo.queue,
					Priority: priority,
				},
			)
			if err != nil {
				tracing.AddErrorToSpan(span, err)
				tracing.AddErrorToSpan(dbSpan, err)
				dbSpan.End()
				return errors.WithStack(err)
			}
			tracing.AddSuccessToSpan(dbSpan)
			dbSpan.End()
		}
		tracing.AddSuccessToSpan(span)
	case UpdateJobSchedulingInfo:
		span.SetAttributes(
			attribute.String("db.table", "jobs"),
			attribute.Int("db.updates.count", len(o)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("UPDATE")...)

		updateJobInfoSqlStatement := "update jobs set scheduling_info = $1::bytea, scheduling_info_version = $2::int where job_id = $3 and $2::int > scheduling_info_version"

		batch := &pgx.Batch{}
		for key, value := range o {
			batch.Queue(updateJobInfoSqlStatement, value.JobSchedulingInfo, value.JobSchedulingInfoVersion, key)
		}

		err := execBatch(ctx, tx, batch)
		if err != nil {
			tracing.AddErrorToSpan(span, err)
			return errors.WithStack(err)
		}
		tracing.AddSuccessToSpan(span)
	case UpdateJobQueuedState:
		span.SetAttributes(
			attribute.String("db.table", "jobs"),
			attribute.Int("db.updates.count", len(o)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("UPDATE")...)

		updateQueuedStateSqlStatement := "update jobs set queued = $1::bool, queued_version = $2::int where job_id = $3 and $2::int > queued_version"

		batch := &pgx.Batch{}
		for key, value := range o {
			batch.Queue(updateQueuedStateSqlStatement, value.Queued, value.QueuedStateVersion, key)
		}

		err := execBatch(ctx, tx, batch)
		if err != nil {
			tracing.AddErrorToSpan(span, err)
			return errors.WithStack(err)
		}
		tracing.AddSuccessToSpan(span)
	case MarkJobSetsCancelRequested:
		span.SetAttributes(
			attribute.String("db.table", "jobs"),
			attribute.String("db.operation", "mark_jobsets_cancel_requested"),
			attribute.Int("db.jobsets.count", len(o.jobSets)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("UPDATE")...)

		for jobSetInfo, cancelDetails := range o.jobSets {
			// If cancelling both queued and leased jobs, avoid ANY([true,false]) redundancy
			if cancelDetails.cancelQueued && cancelDetails.cancelLeased {
				dbCtx, dbSpan := tracing.StartSpan(ctx.Context, "scheduler-ingester", "db-cancel-jobset-all")
				dbSpan.SetAttributes(
					attribute.String("db.operation", "UPDATE"),
					attribute.String("db.table", "jobs"),
					attribute.String("armada.queue", jobSetInfo.queue),
					attribute.String("armada.jobset", jobSetInfo.jobSet),
				)
				err := queries.MarkJobsCancelRequestedBySet(
					dbCtx,
					schedulerdb.MarkJobsCancelRequestedBySetParams{
						Queue:      jobSetInfo.queue,
						JobSet:     jobSetInfo.jobSet,
						CancelUser: &o.cancelUser,
					},
				)
				if err != nil {
					tracing.AddErrorToSpan(span, err)
					tracing.AddErrorToSpan(dbSpan, err)
					dbSpan.End()
					return errors.WithStack(err)
				}
				tracing.AddSuccessToSpan(dbSpan)
				dbSpan.End()
			} else {
				queuedStatesToCancel := make([]bool, 0, 2)
				if cancelDetails.cancelQueued {
					queuedStatesToCancel = append(queuedStatesToCancel, true)
				}
				if cancelDetails.cancelLeased {
					queuedStatesToCancel = append(queuedStatesToCancel, false)
				}
				err := queries.MarkJobsCancelRequestedBySetAndQueuedState(
					ctx,
					schedulerdb.MarkJobsCancelRequestedBySetAndQueuedStateParams{
						Queue:        jobSetInfo.queue,
						JobSet:       jobSetInfo.jobSet,
						QueuedStates: queuedStatesToCancel,
						CancelUser:   &o.cancelUser,
					},
				)
				if err != nil {
					tracing.AddErrorToSpan(span, err)
					return errors.WithStack(err)
				}
			}
		}
		tracing.AddSuccessToSpan(span)
	case MarkJobsCancelRequested:
		span.SetAttributes(
			attribute.String("db.table", "jobs"),
			attribute.String("db.operation", "mark_jobs_cancel_requested"),
			attribute.Int("db.jobsets.count", len(o.jobIds)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("UPDATE")...)

		for key, value := range o.jobIds {
			dbCtx, dbSpan := tracing.StartSpan(ctx.Context, "scheduler-ingester", "db-cancel-jobs-by-id")
			dbSpan.SetAttributes(
				attribute.String("db.operation", "UPDATE"),
				attribute.String("db.table", "jobs"),
				attribute.String("armada.queue", key.queue),
				attribute.String("armada.jobset", key.jobSet),
				attribute.StringSlice("armada.job_ids", value),
				attribute.Int("armada.jobs.count", len(value)),
			)
			params := schedulerdb.MarkJobsCancelRequestedByIdParams{
				Queue:      key.queue,
				JobSet:     key.jobSet,
				JobIds:     value,
				CancelUser: &o.cancelUser,
			}
			err := queries.MarkJobsCancelRequestedById(dbCtx, params)
			if err != nil {
				tracing.AddErrorToSpan(span, err)
				tracing.AddErrorToSpan(dbSpan, err)
				dbSpan.End()
				return errors.WithStack(err)
			}
			tracing.AddSuccessToSpan(dbSpan)
			dbSpan.End()
		}
		tracing.AddSuccessToSpan(span)
	case MarkJobsCancelled:
		span.SetAttributes(
			attribute.String("db.table", "jobs,runs"),
			attribute.String("db.operation", "mark_jobs_cancelled"),
			attribute.Int("db.jobs.count", len(o)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("UPDATE")...)

		jobIds := maps.Keys(o)
		if err := queries.MarkJobsCancelledById(ctx, jobIds); err != nil {
			tracing.AddErrorToSpan(span, err)
			return errors.WithStack(err)
		}
		cancelTimes := make([]interface{}, 0, len(jobIds))
		canceled := make([]bool, 0, len(jobIds))
		for _, jobId := range jobIds {
			cancelTimes = append(cancelTimes, o[jobId])
			canceled = append(canceled, true)
		}
		sqlStmt := multiColumnRunsUpdateStmt("job_id", "cancelled", "terminated_timestamp")
		// order of arguments is important. See multiColumnRunsUpdateStmt function for details
		if _, err := tx.Exec(ctx, sqlStmt, jobIds, canceled, cancelTimes); err != nil {
			tracing.AddErrorToSpan(span, err)
			return errors.WithStack(err)
		}
		tracing.AddSuccessToSpan(span)
	case MarkRunsForJobPreemptRequested:
		span.SetAttributes(
			attribute.String("db.table", "runs"),
			attribute.String("db.operation", "mark_runs_preempt_requested"),
			attribute.Int("db.jobsets.count", len(o)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("UPDATE")...)

		for key, value := range o {
			params := schedulerdb.MarkJobRunsPreemptRequestedByJobIdParams{
				Queue:  key.queue,
				JobSet: key.jobSet,
				JobIds: value,
			}
			err := queries.MarkJobRunsPreemptRequestedByJobId(ctx, params)
			if err != nil {
				tracing.AddErrorToSpan(span, err)
				return errors.WithStack(err)
			}
		}
		tracing.AddSuccessToSpan(span)
	case MarkJobsSucceeded:
		span.SetAttributes(
			attribute.String("db.table", "jobs"),
			attribute.String("db.operation", "mark_jobs_succeeded"),
			attribute.Int("db.jobs.count", len(o)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("UPDATE")...)

		jobIds := maps.Keys(o)
		err := queries.MarkJobsSucceededById(ctx, jobIds)
		if err != nil {
			tracing.AddErrorToSpan(span, err)
			return errors.WithStack(err)
		}
		tracing.AddSuccessToSpan(span)
	case MarkJobsFailed:
		span.SetAttributes(
			attribute.String("db.table", "jobs"),
			attribute.String("db.operation", "mark_jobs_failed"),
			attribute.Int("db.jobs.count", len(o)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("UPDATE")...)

		jobIds := maps.Keys(o)
		err := queries.MarkJobsFailedById(ctx, jobIds)
		if err != nil {
			tracing.AddErrorToSpan(span, err)
			return errors.WithStack(err)
		}
		tracing.AddSuccessToSpan(span)
	case *UpdateJobPriorities:
		uniqueJobIds := slices.Unique(o.jobIds)
		dbCtx, dbSpan := tracing.StartSpan(ctx, "db", "db-operation-update-job-priority")
		dbSpan.SetAttributes(
			attribute.String("db.operation", "update_job_priority"),
			attribute.String("armada.queue", o.key.queue),
			attribute.String("armada.jobset", o.key.jobSet),
			attribute.Int64("armada.priority.new", o.key.Priority),
			attribute.StringSlice("armada.job_ids", uniqueJobIds),
			attribute.Int("armada.jobs.count", len(uniqueJobIds)),
		)
		dbSpan.SetAttributes(tracing.DatabaseAttributes("UPDATE")...)

		err := queries.UpdateJobPriorityById(dbCtx, schedulerdb.UpdateJobPriorityByIdParams{
			Queue:    o.key.queue,
			JobSet:   o.key.jobSet,
			Priority: o.key.Priority,
			JobIds:   uniqueJobIds,
		})
		if err != nil {
			tracing.AddErrorToSpan(dbSpan, err)
			dbSpan.End()
			return errors.WithStack(err)
		}
		tracing.AddSuccessToSpan(dbSpan)
		dbSpan.End()
	case MarkRunsSucceeded:
		span.SetAttributes(
			attribute.String("db.table", "runs"),
			attribute.String("db.operation", "mark_runs_succeeded"),
			attribute.Int("db.runs.count", len(o)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("UPDATE")...)

		successTimes := make([]interface{}, 0, len(o))
		succeeded := make([]bool, 0, len(o))
		runIds := make([]string, 0, len(o))
		for runId, successTime := range o {
			successTimes = append(successTimes, successTime)
			runIds = append(runIds, runId)
			succeeded = append(succeeded, true)
		}
		sqlStmt := multiColumnRunsUpdateStmt("run_id", "succeeded", "terminated_timestamp")
		// order of arguments is important. See multiColumnRunsUpdateStmt function for details
		if _, err := tx.Exec(ctx, sqlStmt, runIds, succeeded, successTimes); err != nil {
			tracing.AddErrorToSpan(span, err)
			return errors.WithStack(err)
		}
		tracing.AddSuccessToSpan(span)
	case MarkRunsFailed:
		span.SetAttributes(
			attribute.String("db.table", "runs"),
			attribute.String("db.operation", "mark_runs_failed"),
			attribute.Int("db.runs.count", len(o)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("UPDATE")...)

		runIds := make([]string, 0, len(o))
		failTimes := make([]interface{}, 0, len(o))
		failed := make([]bool, 0, len(o))
		returned := make([]string, 0)
		runAttempted := make([]string, 0)
		for k, v := range o {
			runIds = append(runIds, k)
			failTimes = append(failTimes, v.FailureTime)
			failed = append(failed, true)
			if v.LeaseReturned {
				returned = append(returned, k)
			}
			if v.RunAttempted {
				runAttempted = append(runAttempted, k)
			}
		}
		sqlStmt := multiColumnRunsUpdateStmt("run_id", "failed", "terminated_timestamp")
		// order of arguments is important. See multiColumnRunsUpdateStmt function for details
		if _, err := tx.Exec(ctx, sqlStmt, runIds, failed, failTimes); err != nil {
			tracing.AddErrorToSpan(span, err)
			return errors.WithStack(err)
		}

		if err := queries.MarkJobRunsReturnedById(ctx, returned); err != nil {
			tracing.AddErrorToSpan(span, err)
			return errors.WithStack(err)
		}
		if err := queries.MarkJobRunsAttemptedById(ctx, runAttempted); err != nil {
			tracing.AddErrorToSpan(span, err)
			return errors.WithStack(err)
		}
		tracing.AddSuccessToSpan(span)
	case MarkRunsRunning:
		span.SetAttributes(
			attribute.String("db.table", "runs"),
			attribute.String("db.operation", "mark_runs_running"),
			attribute.Int("db.runs.count", len(o)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("UPDATE")...)

		runIds := make([]string, 0, len(o))
		runningTimes := make([]interface{}, 0, len(runIds))
		running := make([]bool, 0, len(runIds))
		for runId, failTime := range o {
			runIds = append(runIds, runId)
			runningTimes = append(runningTimes, failTime)
			running = append(running, true)
		}
		sqlStmt := multiColumnRunsUpdateStmt("run_id", "running", "running_timestamp")
		// order of arguments is important. See multiColumnRunsUpdateStmt function for details
		if _, err := tx.Exec(ctx, sqlStmt, runIds, running, runningTimes); err != nil {
			tracing.AddErrorToSpan(span, err)
			return errors.WithStack(err)
		}
		tracing.AddSuccessToSpan(span)
	case MarkRunsPending:
		span.SetAttributes(
			attribute.String("db.table", "runs"),
			attribute.String("db.operation", "mark_runs_pending"),
			attribute.Int("db.runs.count", len(o)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("UPDATE")...)

		runIds := make([]string, 0, len(o))
		pendingTimes := make([]interface{}, 0, len(o))
		pending := make([]bool, 0, len(o))
		for runId, pendingTime := range o {
			runIds = append(runIds, runId)
			pendingTimes = append(pendingTimes, pendingTime)
			pending = append(pending, true)
		}
		sqlStmt := multiColumnRunsUpdateStmt("run_id", "pending", "pending_timestamp")
		if _, err := tx.Exec(ctx, sqlStmt, runIds, pending, pendingTimes); err != nil {
			tracing.AddErrorToSpan(span, err)
			return errors.WithStack(err)
		}
		tracing.AddSuccessToSpan(span)
	case MarkRunsPreempted:
		span.SetAttributes(
			attribute.String("db.table", "runs"),
			attribute.String("db.operation", "mark_runs_preempted"),
			attribute.Int("db.runs.count", len(o)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("UPDATE")...)

		runIds := make([]string, 0, len(o))
		preemptedTimes := make([]interface{}, 0, len(o))
		preempted := make([]bool, 0, len(o))
		for runId, preemptedTime := range o {
			runIds = append(runIds, runId)
			preemptedTimes = append(preemptedTimes, preemptedTime)
			preempted = append(preempted, true)
		}
		sqlStmt := multiColumnRunsUpdateStmt("run_id", "preempted", "preempted_timestamp")
		if _, err := tx.Exec(ctx, sqlStmt, runIds, preempted, preemptedTimes); err != nil {
			tracing.AddErrorToSpan(span, err)
			return errors.WithStack(err)
		}
		tracing.AddSuccessToSpan(span)
	case InsertJobRunErrors:
		span.SetAttributes(
			attribute.String("db.table", "job_run_errors"),
			attribute.String("db.operation", "insert_job_run_errors"),
			attribute.Int("db.errors.count", len(o)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("INSERT")...)

		records := make([]any, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		err := database.Upsert(ctx, tx, "job_run_errors", records)
		if err != nil {
			tracing.AddErrorToSpan(span, err)
			return err
		}
		tracing.AddSuccessToSpan(span)
		return nil
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
	case MarkJobsValidated:
		span.SetAttributes(
			attribute.String("db.table", "jobs"),
			attribute.String("db.operation", "mark_jobs_validated"),
			attribute.Int("db.jobs.count", len(o)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("UPDATE")...)

		markValidatedSqlStatement := `UPDATE jobs SET validated = true, pools = $1 WHERE job_id = $2`
		batch := &pgx.Batch{}
		for key, value := range o {
			batch.Queue(markValidatedSqlStatement, value, key)
		}
		err := execBatch(ctx, tx, batch)
		if err != nil {
			tracing.AddErrorToSpan(span, err)
			return errors.WithStack(err)
		}
		tracing.AddSuccessToSpan(span)
	case UpsertExecutorSettings:
		for _, settingsUpsert := range o {
			err := queries.UpsertExecutorSettings(ctx, schedulerdb.UpsertExecutorSettingsParams{
				ExecutorID:   settingsUpsert.ExecutorID,
				Cordoned:     settingsUpsert.Cordoned,
				CordonReason: settingsUpsert.CordonReason,
				SetByUser:    settingsUpsert.SetByUser,
				SetAtTime:    settingsUpsert.SetAtTime,
			})
			if err != nil {
				return errors.Wrapf(err, "error upserting executor settings for %s", settingsUpsert.ExecutorID)
			}
		}
		return nil
	case DeleteExecutorSettings:
		for _, settingsUpsert := range o {
			err := queries.DeleteExecutorSettings(ctx, settingsUpsert.ExecutorID)
			if err != nil {
				return errors.Wrapf(err, "error deleting executor settings for %s", settingsUpsert.ExecutorID)
			}
		}
		return nil
	case CancelExecutor:
		for executor, cancelRequest := range o {
			jobs, err := queries.SelectJobsByExecutorAndQueues(ctx, schedulerdb.SelectJobsByExecutorAndQueuesParams{
				Executor: executor,
				Queues:   cancelRequest.Queues,
			})
			if err != nil {
				return errors.Wrapf(err, "error cancelling jobs on executor %s by queue and priority class", executor)
			}
			inPriorityClasses := jobInPriorityClasses(cancelRequest.PriorityClasses)
			jobsToCancel := make([]schedulerdb.Job, 0)
			for _, job := range jobs {
				ok, err := inPriorityClasses(job)
				if err != nil {
					return errors.Wrapf(err, "error cancelling jobs on executor %s by queue and priority class", executor)
				}
				if ok {
					jobsToCancel = append(jobsToCancel, job)
				}
			}
			for _, requestCancelParams := range createMarkJobsCancelRequestedByIdParams(jobsToCancel) {
				err = queries.MarkJobsCancelRequestedById(ctx, *requestCancelParams)
				if err != nil {
					return errors.Wrapf(err, "error cancelling jobs on executor %s by queue and priority class", executor)
				}
			}
		}
	case PreemptExecutor:
		for executor, preemptRequest := range o {
			jobs, err := queries.SelectJobsByExecutorAndQueues(ctx, schedulerdb.SelectJobsByExecutorAndQueuesParams{
				Executor: executor,
				Queues:   preemptRequest.Queues,
			})
			if err != nil {
				return errors.Wrapf(err, "error preempting jobs on executor %s by queue and priority class", executor)
			}
			inPriorityClasses := jobInPriorityClasses(preemptRequest.PriorityClasses)
			jobsToPreempt := make([]schedulerdb.Job, 0)
			for _, job := range jobs {
				ok, err := inPriorityClasses(job)
				if err != nil {
					return errors.Wrapf(err, "error preempting jobs on executor %s by queue and priority class", executor)
				}
				if ok {
					jobsToPreempt = append(jobsToPreempt, job)
				}
			}
			for _, requestPreemptParams := range createMarkJobRunsPreemptRequestedByJobIdParams(jobsToPreempt) {
				err = queries.MarkJobRunsPreemptRequestedByJobId(ctx, *requestPreemptParams)
				if err != nil {
					return errors.Wrapf(err, "error preempting jobs on executor %s by queue and priority class", executor)
				}
			}
		}
	case CancelQueue:
		for _, cancelRequest := range o {
			jobs, err := s.selectAllJobsByQueueAndJobState(ctx, queries, cancelRequest.Name, cancelRequest.JobStates)
			if err != nil {
				return errors.Wrapf(err, "error cancelling jobs by queue, job state and priority class")
			}
			inPriorityClasses := jobInPriorityClasses(cancelRequest.PriorityClasses)
			jobsToCancel := make([]schedulerdb.Job, 0)
			for _, job := range jobs {
				ok, err := inPriorityClasses(job)
				if err != nil {
					return errors.Wrapf(err, "error cancelling jobs by queue, job state and priority class")
				}
				if ok {
					jobsToCancel = append(jobsToCancel, job)
				}
			}
			for _, requestCancelParams := range createMarkJobsCancelRequestedByIdParams(jobsToCancel) {
				err = queries.MarkJobsCancelRequestedById(ctx, *requestCancelParams)
				if err != nil {
					return errors.Wrapf(err, "error cancelling jobs by queue, job state and priority class")
				}
			}
		}
	case PreemptQueue:
		for _, preemptRequest := range o {
			// Only allocated jobs can be preempted
			jobStates := []controlplaneevents.ActiveJobState{controlplaneevents.ActiveJobState_LEASED, controlplaneevents.ActiveJobState_PENDING, controlplaneevents.ActiveJobState_RUNNING}
			jobs, err := s.selectAllJobsByQueueAndJobState(ctx, queries, preemptRequest.Name, jobStates)
			if err != nil {
				return errors.Wrapf(err, "error preempting jobs by queue, job state and priority class")
			}
			inPriorityClasses := jobInPriorityClasses(preemptRequest.PriorityClasses)
			jobsToPreempt := make([]schedulerdb.Job, 0)
			for _, job := range jobs {
				ok, err := inPriorityClasses(job)
				if err != nil {
					return errors.Wrapf(err, "error preempting jobs by queue, job state and priority class")
				}
				if ok {
					jobsToPreempt = append(jobsToPreempt, job)
				}
			}
			for _, requestPreemptParams := range createMarkJobRunsPreemptRequestedByJobIdParams(jobsToPreempt) {
				err = queries.MarkJobRunsPreemptRequestedByJobId(ctx, *requestPreemptParams)
				if err != nil {
					return errors.Wrapf(err, "error preempting jobs by queue, job state and priority class")
				}
			}
		}
	default:
		span.SetAttributes(
			attribute.String("db.operation", fmt.Sprintf("unknown_operation_%T", op)),
		)
		span.SetAttributes(tracing.DatabaseAttributes("UNKNOWN")...)
		tracing.AddErrorToSpan(span, errors.Errorf("received unexpected op %+v", op))
		return errors.Errorf("received unexpected op %+v", op)
	}
	return nil
}

func (s *SchedulerDb) selectAllJobsByQueueAndJobState(ctx *armadacontext.Context, queries *schedulerdb.Queries, queue string, jobStates []controlplaneevents.ActiveJobState) ([]schedulerdb.Job, error) {
	items := []schedulerdb.Job{}
	for _, state := range jobStates {
		var jobs []schedulerdb.Job
		var err error
		switch state {
		case controlplaneevents.ActiveJobState_QUEUED:
			jobs, err = queries.SelectQueuedJobsByQueue(ctx, []string{queue})
		case controlplaneevents.ActiveJobState_LEASED:
			jobs, err = queries.SelectLeasedJobsByQueue(ctx, []string{queue})
		case controlplaneevents.ActiveJobState_PENDING:
			jobs, err = queries.SelectPendingJobsByQueue(ctx, []string{queue})
		case controlplaneevents.ActiveJobState_RUNNING:
			jobs, err = queries.SelectRunningJobsByQueue(ctx, []string{queue})
		default:
			return nil, fmt.Errorf("unknown active job state %+v", state)
		}

		if err != nil {
			return nil, fmt.Errorf("unable to select jobs by queue and job state: %s", err)
		}
		items = append(items, jobs...)
	}
	return items, nil
}

// createMarkJobCancelRequestedById returns []*schedulerdb.MarkJobsCancelRequestedByIdParams for the specified jobs such
// that no two MarkJobsCancelRequestedByIdParams are for the same queue and jobset
func createMarkJobsCancelRequestedByIdParams(jobs []schedulerdb.Job) []*schedulerdb.MarkJobsCancelRequestedByIdParams {
	result := make([]*schedulerdb.MarkJobsCancelRequestedByIdParams, 0)
	mapping := map[string]map[string]*schedulerdb.MarkJobsCancelRequestedByIdParams{}
	for _, job := range jobs {
		if _, ok := mapping[job.Queue]; !ok {
			mapping[job.Queue] = map[string]*schedulerdb.MarkJobsCancelRequestedByIdParams{}
		}
		if _, ok := mapping[job.Queue][job.JobSet]; !ok {
			mapping[job.Queue][job.JobSet] = &schedulerdb.MarkJobsCancelRequestedByIdParams{
				Queue:  job.Queue,
				JobSet: job.JobSet,
				JobIds: make([]string, 0),
			}
			result = append(result, mapping[job.Queue][job.JobSet])
		}

		mapping[job.Queue][job.JobSet].JobIds = append(mapping[job.Queue][job.JobSet].JobIds, job.JobID)
	}

	return result
}

// createMarkJobRunsPreemptRequestedByJobIdParams returns []schedulerdb.MarkJobRunsPreemptRequestedByJobIdParams for the specified jobs such
// that no two MarkJobRunsPreemptRequestedByJobIdParams are for the same queue and jobset
func createMarkJobRunsPreemptRequestedByJobIdParams(jobs []schedulerdb.Job) []*schedulerdb.MarkJobRunsPreemptRequestedByJobIdParams {
	result := make([]*schedulerdb.MarkJobRunsPreemptRequestedByJobIdParams, 0)
	mapping := map[string]map[string]*schedulerdb.MarkJobRunsPreemptRequestedByJobIdParams{}
	for _, job := range jobs {
		if _, ok := mapping[job.Queue]; !ok {
			mapping[job.Queue] = map[string]*schedulerdb.MarkJobRunsPreemptRequestedByJobIdParams{}
		}
		if _, ok := mapping[job.Queue][job.JobSet]; !ok {
			mapping[job.Queue][job.JobSet] = &schedulerdb.MarkJobRunsPreemptRequestedByJobIdParams{
				Queue:  job.Queue,
				JobSet: job.JobSet,
				JobIds: make([]string, 0),
			}
			result = append(result, mapping[job.Queue][job.JobSet])
		}

		mapping[job.Queue][job.JobSet].JobIds = append(mapping[job.Queue][job.JobSet].JobIds, job.JobID)
	}

	return result
}

func jobInPriorityClasses(priorityClasses []string) func(schedulerdb.Job) (bool, error) {
	priorityClassMap := map[string]bool{}
	for _, priorityClass := range priorityClasses {
		priorityClassMap[priorityClass] = true
	}
	return func(job schedulerdb.Job) (bool, error) {
		schedulingInfo := &schedulerobjects.JobSchedulingInfo{}
		if err := proto.Unmarshal(job.SchedulingInfo, schedulingInfo); err != nil {
			err = errors.Wrapf(err, "error unmarshalling scheduling info for job %s", job.JobID)
			return false, err
		}

		_, ok := priorityClassMap[schedulingInfo.PriorityClassName]
		return ok, nil
	}
}

// getLockKey is a local method to determine a lock key for the provided set of operations. An error will be returned
// if the operations don't have an associated lock key. Currently, only jobSet events require locking as they rely
// on row sequence numbers being monotonically increasing.
func getLockKey(operations []DbOperation) (int, error) {
	for _, op := range operations {
		if op.GetOperation() == JobSetOperation {
			return JobSetEventsLockKey, nil
		}
	}
	return InvalidLockKey, errors.Errorf("provided operations do not require locking")
}

func execBatch(ctx *armadacontext.Context, tx pgx.Tx, batch *pgx.Batch) error {
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

func multiColumnRunsUpdateStmt(id, phaseColumn, timeStampColumn string) string {
	return fmt.Sprintf(`update runs set
	%[2]v = runs_temp.%[2]v,
	%[3]v = runs_temp.%[3]v
	from (select * from unnest($1::%[4]v[], $2::boolean[] ,$3::timestamptz[]))
	as runs_temp(%[1]v, %[2]v, %[3]v)
	where runs.%[1]v = runs_temp.%[1]v;`,
		id, phaseColumn, timeStampColumn, "text")
}
