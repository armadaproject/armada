package lookoutdb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/model"
)

type LookoutDb struct {
	db          *pgxpool.Pool
	metrics     *metrics.Metrics
	maxAttempts int
	maxBackoff  int
}

func NewLookoutDb(db *pgxpool.Pool, metrics *metrics.Metrics, maxAttempts int, maxBackoff int) *LookoutDb {
	return &LookoutDb{db: db, metrics: metrics, maxAttempts: maxAttempts, maxBackoff: maxBackoff}
}

// Store updates the lookout database according to the supplied InstructionSet.
// The updates are applied in the following order:
// * New Job Creations
// * Job Updates, New Job Creations, New User Annotations
// * Job Run Updates
// In each case we first try to bach insert the rows using the postgres copy protocol.  If this fails then we try a
// slower, serial insert and discard any rows that cannot be inserted.
func (l *LookoutDb) Store(ctx context.Context, instructions *model.InstructionSet) error {
	// We might have multiple updates for the same job or job run
	// These can be conflated to help performance
	jobsToUpdate := conflateJobUpdates(instructions.JobsToUpdate)
	jobRunsToUpdate := conflateJobRunUpdates(instructions.JobRunsToUpdate)

	// Jobs need to be ingested first as other updates may reference these
	l.CreateJobs(ctx, instructions.JobsToCreate)

	// Now we can job updates, annotations and new job runs
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		l.UpdateJobs(ctx, jobsToUpdate)
	}()
	go func() {
		defer wg.Done()
		l.CreateJobRuns(ctx, instructions.JobRunsToCreate)
	}()
	go func() {
		defer wg.Done()
		l.CreateUserAnnotations(ctx, instructions.UserAnnotationsToCreate)
	}()

	wg.Wait()

	// Finally, we can update the job runs
	l.UpdateJobRuns(ctx, jobRunsToUpdate)
	return nil
}

func (l *LookoutDb) CreateJobs(ctx context.Context, instructions []*model.CreateJobInstruction) {
	if len(instructions) == 0 {
		return
	}
	err := l.CreateJobsBatch(ctx, instructions)
	if err != nil {
		log.WithError(err).Warn("Creating jobs via batch failed, will attempt to insert serially (this might be slow).")
		l.CreateJobsScalar(ctx, instructions)
	}
}

func (l *LookoutDb) UpdateJobs(ctx context.Context, instructions []*model.UpdateJobInstruction) {
	if len(instructions) == 0 {
		return
	}
	instructions = l.filterEventsForTerminalJobs(ctx, l.db, instructions, l.metrics)
	err := l.UpdateJobsBatch(ctx, instructions)
	if err != nil {
		log.WithError(err).Warn("Updating jobs via batch failed, will attempt to insert serially (this might be slow).")
		l.UpdateJobsScalar(ctx, instructions)
	}
}

func (l *LookoutDb) CreateJobRuns(ctx context.Context, instructions []*model.CreateJobRunInstruction) {
	if len(instructions) == 0 {
		return
	}
	err := l.CreateJobRunsBatch(ctx, instructions)
	if err != nil {
		log.WithError(err).Warn("Creating job runs via batch failed, will attempt to insert serially (this might be slow).")
		l.CreateJobRunsScalar(ctx, instructions)
	}
}

func (l *LookoutDb) UpdateJobRuns(ctx context.Context, instructions []*model.UpdateJobRunInstruction) {
	if len(instructions) == 0 {
		return
	}
	err := l.UpdateJobRunsBatch(ctx, instructions)
	if err != nil {
		log.WithError(err).Warn("Updating job runs via batch failed, will attempt to insert serially (this might be slow).")
		l.UpdateJobRunsScalar(ctx, instructions)
	}
}

func (l *LookoutDb) CreateUserAnnotations(ctx context.Context, instructions []*model.CreateUserAnnotationInstruction) {
	if len(instructions) == 0 {
		return
	}
	err := l.CreateUserAnnotationsBatch(ctx, instructions)
	if err != nil {
		log.WithError(err).Warn("Creating user annotations via batch failed, will attempt to insert serially (this might be slow).")
		l.CreateUserAnnotationsScalar(ctx, instructions)
	}
}

func (l *LookoutDb) CreateJobsBatch(ctx context.Context, instructions []*model.CreateJobInstruction) error {
	return l.withDatabaseRetryInsert(func() error {
		tmpTable := database.UniqueTableName("job")

		createTmp := func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s 
				(
					job_id 	                      varchar(32),
					queue                        varchar(512),
					owner                        varchar(512),
					jobset                       varchar(1024),
					cpu                          bigint,
					memory                       bigint,
					ephemeral_storage            bigint,
					gpu                          bigint,
					priority                     bigint,
					submitted                    timestamp,
					state                        smallint,
					last_transition_time         timestamp,
					last_transition_time_seconds bigint,
					job_spec                     bytea,
					priority_class               varchar(63)
				) ON COMMIT DROP;`, tmpTable))
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationCreateTempTable)
			}
			return err
		}

		insertTmp := func(tx pgx.Tx) error {
			_, err := tx.CopyFrom(ctx,
				pgx.Identifier{tmpTable},
				[]string{
					"job_id",
					"queue",
					"owner",
					"jobset",
					"cpu",
					"memory",
					"ephemeral_storage",
					"gpu",
					"priority",
					"submitted",
					"state",
					"last_transition_time",
					"last_transition_time_seconds",
					"job_spec",
					"priority_class",
				},
				pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
					return []interface{}{
						instructions[i].JobId,
						instructions[i].Queue,
						instructions[i].Owner,
						instructions[i].JobSet,
						instructions[i].Cpu,
						instructions[i].Memory,
						instructions[i].EphemeralStorage,
						instructions[i].Gpu,
						instructions[i].Priority,
						instructions[i].Submitted,
						instructions[i].State,
						instructions[i].LastTransitionTime,
						instructions[i].LastTransitionTimeSeconds,
						instructions[i].JobProto,
						instructions[i].PriorityClass,
					}, nil
				}),
			)
			return err
		}

		copyToDest := func(tx pgx.Tx) error {
			_, err := tx.Exec(
				ctx,
				fmt.Sprintf(`
					INSERT INTO job (
						job_id,
						queue,
						owner,
						jobset,
						cpu,
						memory,
						ephemeral_storage,
						gpu,
						priority,
						submitted,
						state,
						last_transition_time,
						last_transition_time_seconds,
						job_spec,
						priority_class
					) SELECT * from %s
					ON CONFLICT DO NOTHING`, tmpTable),
			)
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationInsert)
			}
			return err
		}

		return batchInsert(ctx, l.db, createTmp, insertTmp, copyToDest)
	})
}

// CreateJobsScalar will insert jobs one by one into the database
func (l *LookoutDb) CreateJobsScalar(ctx context.Context, instructions []*model.CreateJobInstruction) {
	sqlStatement := `INSERT INTO job (
			job_id,
			queue,
			owner,
			jobset,
			cpu,
			memory,
			ephemeral_storage,
			gpu,
			priority,
			submitted,
			state,
			last_transition_time,
			last_transition_time_seconds,
			job_spec,
			priority_class)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		ON CONFLICT DO NOTHING`
	for _, i := range instructions {
		err := l.withDatabaseRetryInsert(func() error {
			_, err := l.db.Exec(ctx, sqlStatement,
				i.JobId,
				i.Queue,
				i.Owner,
				i.JobSet,
				i.Cpu,
				i.Memory,
				i.EphemeralStorage,
				i.Gpu,
				i.Priority,
				i.Submitted,
				i.State,
				i.LastTransitionTime,
				i.LastTransitionTimeSeconds,
				i.JobProto,
				i.PriorityClass)
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationInsert)
			}
			return err
		})
		if err != nil {
			log.WithError(err).Warnf("Create job for job %s, jobset %s failed", i.JobId, i.JobSet)
		}
	}
}

func (l *LookoutDb) UpdateJobsBatch(ctx context.Context, instructions []*model.UpdateJobInstruction) error {
	return l.withDatabaseRetryInsert(func() error {
		tmpTable := database.UniqueTableName("job")

		createTmp := func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s (
					job_id                       varchar(32),
					priority                     bigint,
					state                        smallint,
					cancelled                    timestamp,
					last_transition_time         timestamp,
					last_transition_time_seconds bigint,
					duplicate                    bool,
					latest_run_id                varchar(36)
				) ON COMMIT DROP;`, tmpTable))
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationCreateTempTable)
			}
			return err
		}

		insertTmp := func(tx pgx.Tx) error {
			_, err := tx.CopyFrom(ctx,
				pgx.Identifier{tmpTable},
				[]string{
					"job_id",
					"priority",
					"state",
					"cancelled",
					"last_transition_time",
					"last_transition_time_seconds",
					"duplicate",
					"latest_run_id",
				},
				pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
					return []interface{}{
						instructions[i].JobId,
						instructions[i].Priority,
						instructions[i].State,
						instructions[i].Cancelled,
						instructions[i].LastTransitionTime,
						instructions[i].LastTransitionTimeSeconds,
						instructions[i].Duplicate,
						instructions[i].LatestRunId,
					}, nil
				}),
			)
			return err
		}

		copyToDest := func(tx pgx.Tx) error {
			_, err := tx.Exec(
				ctx,
				fmt.Sprintf(`UPDATE job
					SET
						priority                     = coalesce(tmp.priority, job.priority),
						state                        = coalesce(tmp.state, job.state),
						cancelled                    = coalesce(tmp.cancelled, job.cancelled),
						last_transition_time         = coalesce(tmp.last_transition_time, job.last_transition_time),
						last_transition_time_seconds = coalesce(tmp.last_transition_time_seconds, job.last_transition_time_seconds),
						duplicate                    = coalesce(tmp.duplicate, job.duplicate),
						latest_run_id                = coalesce(tmp.latest_run_id, job.latest_run_id)
					FROM %s as tmp WHERE tmp.job_id = job.job_id`, tmpTable),
			)
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationUpdate)
			}
			return err
		}

		return batchInsert(ctx, l.db, createTmp, insertTmp, copyToDest)
	})
}

func (l *LookoutDb) UpdateJobsScalar(ctx context.Context, instructions []*model.UpdateJobInstruction) {
	sqlStatement := `UPDATE job
		SET
			priority                     = coalesce($2, priority),
			state                        = coalesce($3, state),
			cancelled                    = coalesce($4, cancelled),
			last_transition_time         = coalesce($5, job.last_transition_time),
			last_transition_time_seconds = coalesce($6, job.last_transition_time_seconds),
			duplicate                    = coalesce($7, duplicate),
			latest_run_id                = coalesce($8, job.latest_run_id)
		WHERE job_id = $1`
	for _, i := range instructions {
		err := l.withDatabaseRetryInsert(func() error {
			_, err := l.db.Exec(ctx, sqlStatement,
				i.JobId,
				i.Priority,
				i.State,
				i.Cancelled,
				i.LastTransitionTime,
				i.LastTransitionTimeSeconds,
				i.Duplicate,
				i.LatestRunId)
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationUpdate)
			}
			return err
		})
		if err != nil {
			log.WithError(err).Warnf("Updating job %s failed", i.JobId)
		}
	}
}

func (l *LookoutDb) CreateJobRunsBatch(ctx context.Context, instructions []*model.CreateJobRunInstruction) error {
	return l.withDatabaseRetryInsert(func() error {
		tmpTable := database.UniqueTableName("job_run")

		createTmp := func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE  %s (
					run_id        varchar(36),
					job_id        varchar(32),
					cluster       varchar(512),
					pending       timestamp,
					job_run_state smallint
				) ON COMMIT DROP;`, tmpTable))
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationCreateTempTable)
			}
			return err
		}

		insertTmp := func(tx pgx.Tx) error {
			_, err := tx.CopyFrom(ctx,
				pgx.Identifier{tmpTable},
				[]string{
					"run_id",
					"job_id",
					"cluster",
					"pending",
					"job_run_state",
				},
				pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
					return []interface{}{
						instructions[i].RunId,
						instructions[i].JobId,
						instructions[i].Cluster,
						instructions[i].Pending,
						instructions[i].JobRunState,
					}, nil
				}),
			)
			return err
		}

		copyToDest := func(tx pgx.Tx) error {
			_, err := tx.Exec(
				ctx,
				fmt.Sprintf(`
					INSERT INTO job_run (
						run_id,
						job_id,
						cluster,
						pending,
						job_run_state
					) SELECT * from %s
					ON CONFLICT DO NOTHING`, tmpTable))
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationInsert)
			}
			return err
		}
		return batchInsert(ctx, l.db, createTmp, insertTmp, copyToDest)
	})
}

func (l *LookoutDb) CreateJobRunsScalar(ctx context.Context, instructions []*model.CreateJobRunInstruction) {
	sqlStatement := `INSERT INTO job_run (
			run_id,
			job_id,
			cluster,
			pending,
			job_run_state)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING`
	for _, i := range instructions {
		err := l.withDatabaseRetryInsert(func() error {
			_, err := l.db.Exec(ctx, sqlStatement,
				i.RunId,
				i.JobId,
				i.Cluster,
				i.Pending,
				i.JobRunState)
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationInsert)
			}
			return err
		})
		if err != nil {
			log.WithError(err).Warnf("Create job run for job %s, run %s failed", i.JobId, i.RunId)
		}
	}
}

func (l *LookoutDb) UpdateJobRunsBatch(ctx context.Context, instructions []*model.UpdateJobRunInstruction) error {
	return l.withDatabaseRetryInsert(func() error {
		tmpTable := database.UniqueTableName("job_run")

		createTmp := func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s (
					run_id        varchar(36),
					node          varchar(512),
					started       timestamp,
					finished      timestamp,
				    job_run_state smallint,
					error         bytea,
				    exit_code     int
				) ON COMMIT DROP;`, tmpTable))
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationCreateTempTable)
			}
			return err
		}

		insertTmp := func(tx pgx.Tx) error {
			_, err := tx.CopyFrom(ctx,
				pgx.Identifier{tmpTable},
				[]string{
					"run_id",
					"node",
					"started",
					"finished",
					"job_run_state",
					"error",
					"exit_code",
				},
				pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
					return []interface{}{
						instructions[i].RunId,
						instructions[i].Node,
						instructions[i].Started,
						instructions[i].Finished,
						instructions[i].JobRunState,
						instructions[i].Error,
						instructions[i].ExitCode,
					}, nil
				}),
			)
			return err
		}

		copyToDest := func(tx pgx.Tx) error {
			_, err := tx.Exec(
				ctx,
				fmt.Sprintf(`UPDATE job_run
					SET
						node          = coalesce(tmp.node, job_run.node),
						started       = coalesce(tmp.started, job_run.started),
						finished      = coalesce(tmp.finished, job_run.finished),
						job_run_state = coalesce(tmp.job_run_state, job_run.job_run_state),
						error         = coalesce(tmp.error, job_run.error),
						exit_code     = coalesce(tmp.exit_code, job_run.exit_code)
					FROM %s as tmp where tmp.run_id = job_run.run_id`, tmpTable),
			)
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationUpdate)
			}
			return err
		}

		return batchInsert(ctx, l.db, createTmp, insertTmp, copyToDest)
	})
}

func (l *LookoutDb) UpdateJobRunsScalar(ctx context.Context, instructions []*model.UpdateJobRunInstruction) {
	sqlStatement := `UPDATE job_run
		SET
			node          = coalesce($2, node),
			started       = coalesce($3, started),
			finished      = coalesce($4, finished),
			job_run_state = coalesce($5, job_run_state),
			error         = coalesce($6, error),
			exit_code     = coalesce($7, exit_code)
		WHERE run_id = $1`
	for _, i := range instructions {
		err := l.withDatabaseRetryInsert(func() error {
			_, err := l.db.Exec(ctx, sqlStatement,
				i.RunId,
				i.Node,
				i.Started,
				i.Finished,
				i.JobRunState,
				i.Error,
				i.ExitCode)
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationUpdate)
			}
			return err
		})
		if err != nil {
			log.WithError(err).Warnf("Updating job run %s failed", i.RunId)
		}
	}
}

func (l *LookoutDb) CreateUserAnnotationsBatch(ctx context.Context, instructions []*model.CreateUserAnnotationInstruction) error {
	return l.withDatabaseRetryInsert(func() error {
		tmpTable := database.UniqueTableName("user_annotation_lookup")

		createTmp := func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE  %s (
					job_id varchar(32),
					key    varchar(1024),
					value  varchar(1024),
					queue  varchar(512),
					jobset varchar(1024)
				) ON COMMIT DROP;`, tmpTable))
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationCreateTempTable)
			}
			return err
		}

		insertTmp := func(tx pgx.Tx) error {
			_, err := tx.CopyFrom(ctx,
				pgx.Identifier{tmpTable},
				[]string{
					"job_id",
					"key",
					"value",
					"queue",
					"jobset",
				},
				pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
					return []interface{}{
						instructions[i].JobId,
						instructions[i].Key,
						instructions[i].Value,
						instructions[i].Queue,
						instructions[i].Jobset,
					}, nil
				}),
			)
			return err
		}

		copyToDest := func(tx pgx.Tx) error {
			_, err := tx.Exec(
				ctx,
				fmt.Sprintf(`
					INSERT INTO user_annotation_lookup (
						job_id,
						key,
						value,
						queue,
						jobset
					) SELECT * from %s
					ON CONFLICT DO NOTHING`, tmpTable))
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationInsert)
			}
			return err
		}
		return batchInsert(ctx, l.db, createTmp, insertTmp, copyToDest)
	})
}

func (l *LookoutDb) CreateUserAnnotationsScalar(ctx context.Context, instructions []*model.CreateUserAnnotationInstruction) {
	sqlStatement := `INSERT INTO user_annotation_lookup (
			job_id,
			key,
			value,
			queue,
			jobset)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING`
	for _, i := range instructions {
		err := l.withDatabaseRetryInsert(func() error {
			_, err := l.db.Exec(ctx, sqlStatement,
				i.JobId,
				i.Key,
				i.Value,
				i.Queue,
				i.Jobset)
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationInsert)
			}
			return err
		})
		// TODO- work out what is a retryable error
		if err != nil {
			log.WithError(err).Warnf("Create annotation run for job %s, key %s failed", i.JobId, i.Key)
		}
	}
}

func batchInsert(ctx context.Context, db *pgxpool.Pool, createTmp func(pgx.Tx) error,
	insertTmp func(pgx.Tx) error, copyToDest func(pgx.Tx) error,
) error {
	return db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		// Create a temporary table to hold the staging data
		err := createTmp(tx)
		if err != nil {
			return err
		}

		err = insertTmp(tx)
		if err != nil {
			return err
		}

		err = copyToDest(tx)
		if err != nil {
			return err
		}
		return nil
	})
}

func conflateJobUpdates(updates []*model.UpdateJobInstruction) []*model.UpdateJobInstruction {
	isTerminal := func(p *int32) bool {
		if p == nil {
			return false
		} else {
			return *p == lookout.JobFailedOrdinal ||
				*p == lookout.JobSucceededOrdinal ||
				*p == lookout.JobCancelledOrdinal ||
				*p == lookout.JobPreemptedOrdinal
		}
	}

	updatesById := make(map[string]*model.UpdateJobInstruction)
	for _, update := range updates {
		existing, ok := updatesById[update.JobId]
		if !ok {
			updatesById[update.JobId] = update
			continue
		}

		// Unfortunately once a job has reached a terminal state we still get state updates for it e.g. we can get an event to
		// say it's now "running".  We have to throw these away else we'll end up with a zombie job.
		//
		// Need to deal with preempted jobs separately
		// We could get a JobFailed event (or another terminal event) after a JobPreempted event,
		// but the job should still be marked as Preempted
		if !isTerminal(existing.State) || *update.State == lookout.JobPreemptedOrdinal {
			if update.Priority != nil {
				existing.Priority = update.Priority
			}
			if update.State != nil {
				existing.State = update.State
			}
			if update.Cancelled != nil {
				existing.Cancelled = update.Cancelled
			}
			if update.LastTransitionTime != nil {
				existing.LastTransitionTime = update.LastTransitionTime
			}
			if update.LastTransitionTimeSeconds != nil {
				existing.LastTransitionTimeSeconds = update.LastTransitionTimeSeconds
			}
			if update.Duplicate != nil {
				existing.Duplicate = update.Duplicate
			}
			if update.LatestRunId != nil {
				existing.LatestRunId = update.LatestRunId
			}
		}
	}

	conflated := make([]*model.UpdateJobInstruction, 0, len(updatesById))
	// TODO: it turns out that iteration over a map in go yields a different key order each time!
	// This means that that slice outputted by this function (and the one below) will have a random order
	// This isn't a problem as such for the database but does mean that reproducing errors etc will be hard
	for _, v := range updatesById {
		conflated = append(conflated, v)
	}
	return conflated
}

func conflateJobRunUpdates(updates []*model.UpdateJobRunInstruction) []*model.UpdateJobRunInstruction {
	updatesById := make(map[string]*model.UpdateJobRunInstruction)
	for _, update := range updates {
		existing, ok := updatesById[update.RunId]
		if ok {
			if update.Node != nil {
				existing.Node = update.Node
			}
			if update.Started != nil {
				existing.Started = update.Started
			}
			if update.Finished != nil {
				existing.Finished = update.Finished
			}
			if update.Error != nil {
				existing.Error = update.Error
			}
			if update.JobRunState != nil {
				existing.JobRunState = update.JobRunState
			}
			if update.ExitCode != nil {
				existing.ExitCode = update.ExitCode
			}
		} else {
			updatesById[update.RunId] = update
		}
	}

	conflated := make([]*model.UpdateJobRunInstruction, 0, len(updatesById))
	for _, v := range updatesById {
		conflated = append(conflated, v)
	}
	return conflated
}

// updateInstructionsForJob is used in filterEventsForTerminalJobs, and records a list of job updates for a single job,
// along with whether it contains an instruction corresponding to a JobPreempted event
type updateInstructionsForJob struct {
	instructions      []*model.UpdateJobInstruction
	containsPreempted bool
}

// filterEventsForTerminalJobs queries the database for any jobs that are in a terminal state and removes them from the list of
// instructions.  This is necessary because Armada will generate event statuses even for jobs that have reached a terminal state
// The proper solution here is to make it so once a job is terminal, no more events are generated for it, but until
// that day we have to manually filter them out here.
// NOTE: this function will retry querying the database for as long as possible in order to determine which jobs are
// in the terminal state.  If, however, the database returns a non-retryable error it will give up and simply not
// filter out any events as the job state is undetermined.
func (l *LookoutDb) filterEventsForTerminalJobs(
	ctx context.Context,
	db *pgxpool.Pool,
	instructions []*model.UpdateJobInstruction,
	m *metrics.Metrics,
) []*model.UpdateJobInstruction {
	jobIds := make([]string, len(instructions))
	for i, instruction := range instructions {
		jobIds[i] = instruction.JobId
	}

	rowsRaw, err := l.withDatabaseRetryQuery(func() (interface{}, error) {
		terminalStates := []int{
			lookout.JobSucceededOrdinal,
			lookout.JobFailedOrdinal,
			lookout.JobCancelledOrdinal,
			lookout.JobPreemptedOrdinal,
		}
		return db.Query(ctx, "SELECT DISTINCT job_id, state FROM JOB where state = any($1) AND job_id = any($2)", terminalStates, jobIds)
	})
	if err != nil {
		m.RecordDBError(metrics.DBOperationRead)
		log.WithError(err).Warnf("Cannot retrieve job state from the database- Cancelled jobs may not be filtered out")
		return instructions
	}
	rows := rowsRaw.(pgx.Rows)

	terminalJobs := make(map[string]int)
	for rows.Next() {
		jobId := ""
		var state int16
		err := rows.Scan(&jobId, &state)
		if err != nil {
			log.WithError(err).Warnf("Cannot retrieve jobId from row. Cancelled job will not be filtered out")
		} else {
			terminalJobs[jobId] = int(state)
		}
	}

	if len(terminalJobs) > 0 {
		jobInstructionMap := make(map[string]*updateInstructionsForJob)
		for _, instruction := range instructions {
			data, ok := jobInstructionMap[instruction.JobId]
			if !ok {
				data = &updateInstructionsForJob{
					instructions:      []*model.UpdateJobInstruction{},
					containsPreempted: false,
				}
				jobInstructionMap[instruction.JobId] = data
			}
			data.instructions = append(data.instructions, instruction)
			data.containsPreempted = instruction.State != nil && *instruction.State == lookout.JobPreemptedOrdinal
		}

		var filtered []*model.UpdateJobInstruction
		for jobId, updateInstructions := range jobInstructionMap {
			state, ok := terminalJobs[jobId]
			// Need to record updates if either:
			// * Job is not in a terminal state
			// * Job is in succeeded, failed or cancelled, but job preempted event will be recorded
			if !ok || ((state == lookout.JobSucceededOrdinal ||
				state == lookout.JobFailedOrdinal ||
				state == lookout.JobCancelledOrdinal) && updateInstructions.containsPreempted) {
				filtered = append(filtered, updateInstructions.instructions...)
			}
		}
		return filtered
	} else {
		return instructions
	}
}

func (l *LookoutDb) withDatabaseRetryInsert(executeDb func() error) error {
	_, err := l.withDatabaseRetryQuery(func() (interface{}, error) {
		return nil, executeDb()
	})
	return err
}

// Executes a database function, retrying until it either succeeds or encounters a non-retryable error
func (l *LookoutDb) withDatabaseRetryQuery(executeDb func() (interface{}, error)) (interface{}, error) {
	// TODO: arguably this should come from config
	backOff := 1
	numRetries := 0
	var err error = nil
	for attempt := 0; attempt < l.maxAttempts; attempt++ {
		res, err := executeDb()

		if err == nil {
			return res, nil
		}

		if armadaerrors.IsNetworkError(err) || armadaerrors.IsRetryablePostgresError(err) {
			backOff = min(2*backOff, l.maxBackoff)
			numRetries++
			log.WithError(err).Warnf("Retryable error encountered executing sql, will wait for %d seconds before retrying.", backOff)
			time.Sleep(time.Duration(backOff) * time.Second)
		} else {
			// Non retryable error
			return nil, err
		}
	}

	// If we get to here then we've got an error we can't handle.  Panic
	panic(errors.WithStack(&armadaerrors.ErrMaxRetriesExceeded{
		Message:   fmt.Sprintf("Gave up running database query after %d retries", l.maxAttempts),
		LastError: err,
	}))
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
