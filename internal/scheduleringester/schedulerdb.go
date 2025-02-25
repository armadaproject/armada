package scheduleringester

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/slices"
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
	return ingest.WithRetry(func() (bool, error) {
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

func (s *SchedulerDb) WriteDbOp(ctx *armadacontext.Context, tx pgx.Tx, op DbOperation) error {
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
			records[i] = *v.DbRun
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
		for jobSetInfo, cancelDetails := range o.jobSets {
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
					CancelUser:   &o.cancelUser,
				},
			)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	case MarkJobsCancelRequested:
		for key, value := range o.jobIds {
			params := schedulerdb.MarkJobsCancelRequestedByIdParams{
				Queue:      key.queue,
				JobSet:     key.jobSet,
				JobIds:     value,
				CancelUser: &o.cancelUser,
			}
			err := queries.MarkJobsCancelRequestedById(ctx, params)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	case MarkJobsCancelled:
		jobIds := maps.Keys(o)
		if err := queries.MarkJobsCancelledById(ctx, jobIds); err != nil {
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
			return errors.WithStack(err)
		}
	case MarkRunsForJobPreemptRequested:
		for key, value := range o {
			params := schedulerdb.MarkJobRunsPreemptRequestedByJobIdParams{
				Queue:  key.queue,
				JobSet: key.jobSet,
				JobIds: value,
			}
			err := queries.MarkJobRunsPreemptRequestedByJobId(ctx, params)
			if err != nil {
				return errors.WithStack(err)
			}
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
	case *UpdateJobPriorities:
		err := queries.UpdateJobPriorityById(ctx, schedulerdb.UpdateJobPriorityByIdParams{
			Queue:    o.key.queue,
			JobSet:   o.key.jobSet,
			Priority: o.key.Priority,
			JobIds:   slices.Unique(o.jobIds),
		})
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkRunsSucceeded:
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
			return errors.WithStack(err)
		}
	case MarkRunsFailed:
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
			return errors.WithStack(err)
		}

		if err := queries.MarkJobRunsReturnedById(ctx, returned); err != nil {
			return errors.WithStack(err)
		}
		if err := queries.MarkJobRunsAttemptedById(ctx, runAttempted); err != nil {
			return errors.WithStack(err)
		}
	case MarkRunsRunning:
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
			return errors.WithStack(err)
		}
	case MarkRunsPending:
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
			return errors.WithStack(err)
		}
	case MarkRunsPreempted:
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
	case MarkJobsValidated:
		markValidatedSqlStatement := `UPDATE jobs SET validated = true, pools = $1 WHERE job_id = $2`
		batch := &pgx.Batch{}
		for key, value := range o {
			batch.Queue(markValidatedSqlStatement, value, key)
		}
		err := execBatch(ctx, tx, batch)
		if err != nil {
			return errors.WithStack(err)
		}
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
