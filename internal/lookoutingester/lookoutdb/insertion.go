package lookoutdb

import (
	"fmt"
	"sync"
	"time"

	"github.com/armadaproject/armada/internal/common/context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/lookout/configuration"
	"github.com/armadaproject/armada/internal/lookout/repository"
	"github.com/armadaproject/armada/internal/lookoutingester/model"
)

type LookoutDb struct {
	db      *pgxpool.Pool
	metrics *metrics.Metrics
	config  *configuration.LookoutIngesterConfiguration
}

func NewLookoutDb(
	db *pgxpool.Pool,
	metrics *metrics.Metrics,
	config *configuration.LookoutIngesterConfiguration,
) ingest.Sink[*model.InstructionSet] {
	if config.Debug.DisableConflateDBUpdates {
		log.Warn("config.debug.disableConflateDBUpdates == true. Performance may be negatively impacted.")
	}

	return &LookoutDb{db: db, metrics: metrics, config: config}
}

// Store updates the lookout database according to the supplied InstructionSet.
// The updates are applied in the following order:
// * New Job Creations
// * Job Updates, New Job Creations, New User Annotations
// * Job Run Updates, New Job Containers
// In each case we first try to bach insert the rows using the postgres copy protocol.  If this fails then we try a
// slower, serial insert and discard any rows that cannot be inserted.
func (l *LookoutDb) Store(ctx *context.ArmadaContext, instructions *model.InstructionSet) error {
	jobsToUpdate := instructions.JobsToUpdate
	jobRunsToUpdate := instructions.JobRunsToUpdate

	// We might have multiple updates for the same job or job run
	// These can be conflated to help performance
	if !l.config.Debug.DisableConflateDBUpdates {
		jobsToUpdate = conflateJobUpdates(jobsToUpdate)
		jobRunsToUpdate = conflateJobRunUpdates(jobRunsToUpdate)
	}

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

	// Finally, we can update the job runs and container exit codes
	wg2 := sync.WaitGroup{}
	wg2.Add(2)
	go func() {
		defer wg2.Done()
		l.UpdateJobRuns(ctx, jobRunsToUpdate)
	}()
	go func() {
		defer wg2.Done()
		l.CreateJobRunContainers(ctx, instructions.JobRunContainersToCreate)
	}()
	wg2.Wait()
	return nil
}

func (l *LookoutDb) CreateJobs(ctx *context.ArmadaContext, instructions []*model.CreateJobInstruction) {
	if len(instructions) == 0 {
		return
	}

	if l.config.Debug.DisableConflateDBUpdates {
		l.CreateJobsScalar(ctx, instructions)
		return
	}

	err := l.CreateJobsBatch(ctx, instructions)
	if err != nil {
		log.Warnf("Creating jobs via batch failed, will attempt to insert serially (this might be slow).  Error was %+v", err)
		l.CreateJobsScalar(ctx, instructions)
	}
}

func (l *LookoutDb) UpdateJobs(ctx *context.ArmadaContext, instructions []*model.UpdateJobInstruction) {
	if len(instructions) == 0 {
		return
	}
	instructions = filterEventsForTerminalJobs(ctx, l.db, instructions, l.metrics)

	if l.config.Debug.DisableConflateDBUpdates {
		l.UpdateJobsScalar(ctx, instructions)
		return
	}

	err := l.UpdateJobsBatch(ctx, instructions)
	if err != nil {
		log.Warnf("Updating jobs via batch failed, will attempt to insert serially (this might be slow).  Error was %+v", err)
		l.UpdateJobsScalar(ctx, instructions)
	}
}

func (l *LookoutDb) CreateJobRuns(ctx *context.ArmadaContext, instructions []*model.CreateJobRunInstruction) {
	if len(instructions) == 0 {
		return
	}

	if l.config.Debug.DisableConflateDBUpdates {
		l.CreateJobRunsScalar(ctx, instructions)
		return
	}

	err := l.CreateJobRunsBatch(ctx, instructions)
	if err != nil {
		log.Warnf("Creating job runs via batch failed, will attempt to insert serially (this might be slow).  Error was %+v", err)
		l.CreateJobRunsScalar(ctx, instructions)
	}
}

func (l *LookoutDb) UpdateJobRuns(ctx *context.ArmadaContext, instructions []*model.UpdateJobRunInstruction) {
	if len(instructions) == 0 {
		return
	}

	if l.config.Debug.DisableConflateDBUpdates {
		l.UpdateJobRunsScalar(ctx, instructions)
		return
	}

	err := l.UpdateJobRunsBatch(ctx, instructions)
	if err != nil {
		log.Warnf("Updating job runs via batch failed, will attempt to insert serially (this might be slow).  Error was %+v", err)
		l.UpdateJobRunsScalar(ctx, instructions)
	}
}

func (l *LookoutDb) CreateUserAnnotations(ctx *context.ArmadaContext, instructions []*model.CreateUserAnnotationInstruction) {
	if len(instructions) == 0 {
		return
	}

	if l.config.Debug.DisableConflateDBUpdates {
		l.CreateUserAnnotationsScalar(ctx, instructions)
		return
	}

	err := l.CreateUserAnnotationsBatch(ctx, instructions)
	if err != nil {
		log.Warnf("Creating user annotations via batch failed, will attempt to insert serially (this might be slow).  Error was %+v", err)
		l.CreateUserAnnotationsScalar(ctx, instructions)
	}
}

func (l *LookoutDb) CreateJobRunContainers(ctx *context.ArmadaContext, instructions []*model.CreateJobRunContainerInstruction) {
	if len(instructions) == 0 {
		return
	}

	if l.config.Debug.DisableConflateDBUpdates {
		l.CreateJobRunContainersScalar(ctx, instructions)
		return
	}

	err := l.CreateJobRunContainersBatch(ctx, instructions)
	if err != nil {
		log.Warnf("Creating job run containers via batch failed, will attempt to insert serially (this might be slow).  Error was %+v", err)
		l.CreateJobRunContainersScalar(ctx, instructions)
	}
}

func (l *LookoutDb) CreateJobsBatch(ctx *context.ArmadaContext, instructions []*model.CreateJobInstruction) error {
	return withDatabaseRetryInsert(func() error {
		tmpTable := database.UniqueTableName("job")

		createTmp := func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s
				(
				  job_id 	varchar(32),
				  queue     varchar(512),
				  owner     varchar(512),
				  jobset    varchar(1024),
				  priority  double precision,
	                          submitted timestamp,
	                          orig_job_spec bytea,
				  state     smallint,
				  job_updated   timestamp
				) ON COMMIT DROP;`, tmpTable))
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationCreateTempTable)
			}
			return err
		}

		insertTmp := func(tx pgx.Tx) error {
			_, err := tx.CopyFrom(ctx,
				pgx.Identifier{tmpTable},
				[]string{"job_id", "queue", "owner", "jobset", "priority", "submitted", "orig_job_spec", "state", "job_updated"},
				pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
					return []interface{}{
						instructions[i].JobId,
						instructions[i].Queue,
						instructions[i].Owner,
						instructions[i].JobSet,
						instructions[i].Priority,
						instructions[i].Submitted,
						instructions[i].JobProto,
						instructions[i].State,
						instructions[i].Updated,
					}, nil
				}),
			)
			return err
		}

		copyToDest := func(tx pgx.Tx) error {
			_, err := tx.Exec(
				ctx,
				fmt.Sprintf(`
					INSERT INTO job (job_id, queue, owner, jobset, priority, submitted, orig_job_spec, state, job_updated) SELECT * from %s
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
func (l *LookoutDb) CreateJobsScalar(ctx *context.ArmadaContext, instructions []*model.CreateJobInstruction) {
	sqlStatement := `INSERT INTO job (job_id, queue, owner, jobset, priority, submitted, orig_job_spec, state, job_updated)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT DO NOTHING`
	for _, i := range instructions {
		err := withDatabaseRetryInsert(func() error {
			_, err := l.db.Exec(ctx, sqlStatement, i.JobId, i.Queue, i.Owner, i.JobSet, i.Priority, i.Submitted, i.JobProto, i.State, i.Updated)
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationInsert)
			}
			return err
		})
		if err != nil {
			log.Warnf("Create job for job %s, jobset %s failed with error %+v", i.JobId, i.JobSet, err)
		}
	}
}

func (l *LookoutDb) UpdateJobsBatch(ctx *context.ArmadaContext, instructions []*model.UpdateJobInstruction) error {
	return withDatabaseRetryInsert(func() error {
		tmpTable := database.UniqueTableName("job")

		createTmp := func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s
				(
					job_id      varchar(32),
					priority    double precision,
					state       smallint,
					job_updated timestamp,
					cancelled   timestamp,
					duplicate   bool
				) ON COMMIT DROP;`, tmpTable))
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationCreateTempTable)
			}
			return err
		}

		insertTmp := func(tx pgx.Tx) error {
			_, err := tx.CopyFrom(ctx,
				pgx.Identifier{tmpTable},
				[]string{"job_id", "priority", "state", "job_updated", "cancelled", "duplicate"},
				pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
					return []interface{}{
						instructions[i].JobId,
						instructions[i].Priority,
						instructions[i].State,
						instructions[i].Updated,
						instructions[i].Cancelled,
						instructions[i].Duplicate,
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
				  priority = coalesce(tmp.priority, job.priority),
                  state = coalesce(tmp.state, job.state),
                  job_updated = tmp.job_updated,
                  cancelled = coalesce(tmp.cancelled, job.cancelled),
                  duplicate = coalesce(tmp.duplicate, job.duplicate)
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

func (l *LookoutDb) UpdateJobsScalar(ctx *context.ArmadaContext, instructions []*model.UpdateJobInstruction) {
	sqlStatement := `UPDATE job
				SET
				  priority = coalesce($1, priority),
                  state = coalesce($2, state),
                  job_updated = coalesce($3, job_updated),
                  cancelled = coalesce($4, cancelled),
                  duplicate = coalesce($5, duplicate)
				WHERE job_id = $6`
	for _, i := range instructions {
		err := withDatabaseRetryInsert(func() error {
			_, err := l.db.Exec(ctx, sqlStatement, i.Priority, i.State, i.Updated, i.Cancelled, i.Duplicate, i.JobId)
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationUpdate)
			}
			return err
		})
		if err != nil {
			log.Warnf("Updating job %s failed with error %+v", i.JobId, err)
		}
	}
}

func (l *LookoutDb) CreateJobRunsBatch(ctx *context.ArmadaContext, instructions []*model.CreateJobRunInstruction) error {
	return withDatabaseRetryInsert(func() error {
		tmpTable := database.UniqueTableName("job_run")

		createTmp := func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE  %s
				(
				  run_id  varchar(36),
                  job_id  varchar(32),
				  cluster varchar(512),
				  created timestamp
				) ON COMMIT DROP;`, tmpTable))
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationCreateTempTable)
			}
			return err
		}

		insertTmp := func(tx pgx.Tx) error {
			_, err := tx.CopyFrom(ctx,
				pgx.Identifier{tmpTable},
				[]string{"run_id", "job_id", "created", "cluster"},
				pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
					return []interface{}{
						instructions[i].RunId,
						instructions[i].JobId,
						instructions[i].Created,
						instructions[i].Cluster,
					}, nil
				}),
			)
			return err
		}

		copyToDest := func(tx pgx.Tx) error {
			_, err := tx.Exec(
				ctx,
				fmt.Sprintf(`
					INSERT INTO job_run (run_id, job_id, cluster, created) SELECT * from %s
					ON CONFLICT DO NOTHING`, tmpTable))
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationInsert)
			}
			return err
		}
		return batchInsert(ctx, l.db, createTmp, insertTmp, copyToDest)
	})
}

func (l *LookoutDb) CreateJobRunsScalar(ctx *context.ArmadaContext, instructions []*model.CreateJobRunInstruction) {
	sqlStatement := `INSERT INTO job_run (run_id, job_id, created, cluster)
		 VALUES ($1, $2, $3, $4)
         ON CONFLICT DO NOTHING`
	for _, i := range instructions {
		err := withDatabaseRetryInsert(func() error {
			_, err := l.db.Exec(ctx, sqlStatement, i.RunId, i.JobId, i.Created, i.Cluster)
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationInsert)
			}
			return err
		})
		if err != nil {
			log.Warnf("Create job run for job %s, run %s failed with error %+v", i.JobId, i.RunId, err)
		}
	}
}

func (l *LookoutDb) UpdateJobRunsBatch(ctx *context.ArmadaContext, instructions []*model.UpdateJobRunInstruction) error {
	return withDatabaseRetryInsert(func() error {
		tmpTable := database.UniqueTableName("job_run")

		createTmp := func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s
				(
			      run_id             varchar(36),
			      cluster            varchar(512),
			      node               varchar(512),
			      started            timestamp,
			      finished           timestamp,
			      preempted          timestamp,
			      succeeded          boolean,
			      error              varchar(2048),
			      pod_number         integer,
			      unable_to_schedule boolean
				) ON COMMIT DROP;`, tmpTable))
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationCreateTempTable)
			}
			return err
		}

		insertTmp := func(tx pgx.Tx) error {
			_, err := tx.CopyFrom(ctx,
				pgx.Identifier{tmpTable},
				[]string{"run_id", "node", "started", "finished", "preempted", "succeeded", "error", "pod_number", "unable_to_schedule"},
				pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
					return []interface{}{
						instructions[i].RunId,
						instructions[i].Node,
						instructions[i].Started,
						instructions[i].Finished,
						instructions[i].Preempted,
						instructions[i].Succeeded,
						instructions[i].Error,
						instructions[i].PodNumber,
						instructions[i].UnableToSchedule,
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
		                  node = coalesce(tmp.node, job_run.node),
		                  started = coalesce(tmp.started, job_run.started),
		                  finished = coalesce(tmp.finished, job_run.finished),
		                  succeeded = coalesce(tmp.succeeded, job_run.succeeded),
		                  preempted = coalesce(tmp.preempted, job_run.preempted),
		                  error = coalesce(tmp.error, job_run.error),
		                  pod_number = coalesce(tmp.pod_number, job_run.pod_number),
		                  unable_to_schedule = coalesce(tmp.unable_to_schedule, job_run.unable_to_schedule)
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

func (l *LookoutDb) UpdateJobRunsScalar(ctx *context.ArmadaContext, instructions []*model.UpdateJobRunInstruction) {
	sqlStatement := `UPDATE job_run
				SET
				  node = coalesce($1, node),
				  started = coalesce($2, started),
				  finished = coalesce($3, finished),
				  succeeded = coalesce($4, succeeded),
				  preempted = coalesce($5, preempted),
				  error = coalesce($6, error),
				  pod_number = coalesce($7, pod_number),
				  unable_to_schedule = coalesce($8, unable_to_schedule)
				WHERE run_id = $9`
	for _, i := range instructions {
		err := withDatabaseRetryInsert(func() error {
			_, err := l.db.Exec(ctx, sqlStatement, i.Node, i.Started, i.Finished, i.Succeeded, i.Preempted, i.Error, i.PodNumber, i.UnableToSchedule, i.RunId)
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationUpdate)
			}
			return err
		})
		if err != nil {
			log.Warnf("Updating job run %s failed with error %+v", i.RunId, err)
		}
	}
}

func (l *LookoutDb) CreateUserAnnotationsBatch(ctx *context.ArmadaContext, instructions []*model.CreateUserAnnotationInstruction) error {
	return withDatabaseRetryInsert(func() error {
		tmpTable := database.UniqueTableName("user_annotation_lookup")

		createTmp := func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE  %s
				(
				  job_id  varchar(32),
                  key     varchar(1024),
				  value   varchar(1024)
				) ON COMMIT DROP;`, tmpTable))
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationCreateTempTable)
			}
			return err
		}

		insertTmp := func(tx pgx.Tx) error {
			_, err := tx.CopyFrom(ctx,
				pgx.Identifier{tmpTable},
				[]string{"job_id", "key", "value"},
				pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
					return []interface{}{
						instructions[i].JobId,
						instructions[i].Key,
						instructions[i].Value,
					}, nil
				}),
			)
			return err
		}

		copyToDest := func(tx pgx.Tx) error {
			_, err := tx.Exec(
				ctx,
				fmt.Sprintf(`
					INSERT INTO user_annotation_lookup (job_id, key, value) SELECT * from %s
					ON CONFLICT DO NOTHING`, tmpTable))
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationInsert)
			}
			return err
		}
		return batchInsert(ctx, l.db, createTmp, insertTmp, copyToDest)
	})
}

func (l *LookoutDb) CreateUserAnnotationsScalar(ctx *context.ArmadaContext, instructions []*model.CreateUserAnnotationInstruction) {
	sqlStatement := `INSERT INTO user_annotation_lookup (job_id, key, value)
		 VALUES ($1, $2, $3)
         ON CONFLICT DO NOTHING`
	for _, i := range instructions {
		err := withDatabaseRetryInsert(func() error {
			_, err := l.db.Exec(ctx, sqlStatement, i.JobId, i.Key, i.Value)
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationInsert)
			}
			return err
		})
		// TODO- work out what is a retryable error
		if err != nil {
			log.Warnf("Create annotation run for job %s, key %s failed with error %+v", i.JobId, i.Key, err)
		}
	}
}

func (l *LookoutDb) CreateJobRunContainersBatch(ctx *context.ArmadaContext, instructions []*model.CreateJobRunContainerInstruction) error {
	return withDatabaseRetryInsert(func() error {
		tmpTable := database.UniqueTableName("job_run_container")
		createTmp := func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s (
				  run_id  varchar(36),
                  container_name  varchar(512),
				  exit_code integer
				) ON COMMIT DROP;`, tmpTable))
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationInsert)
			}
			return err
		}

		insertTmp := func(tx pgx.Tx) error {
			_, err := tx.CopyFrom(ctx,
				pgx.Identifier{tmpTable},
				[]string{"run_id", "container_name", "exit_code"},
				pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
					return []interface{}{
						instructions[i].RunId,
						instructions[i].ContainerName,
						instructions[i].ExitCode,
					}, nil
				}),
			)
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationInsert)
			}
			return err
		}

		copyToDest := func(tx pgx.Tx) error {
			_, err := tx.Exec(
				ctx,
				fmt.Sprintf(`
					INSERT INTO job_run_container (run_id, container_name, exit_code) SELECT * from %s
					ON CONFLICT DO NOTHING`, tmpTable))
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationInsert)
			}
			return err
		}
		return batchInsert(ctx, l.db, createTmp, insertTmp, copyToDest)
	})
}

func (l *LookoutDb) CreateJobRunContainersScalar(ctx *context.ArmadaContext, instructions []*model.CreateJobRunContainerInstruction) {
	sqlStatement := `INSERT INTO job_run_container (run_id, container_name, exit_code)
		 VALUES ($1, $2, $3)
	     ON CONFLICT DO NOTHING`
	for _, i := range instructions {
		err := withDatabaseRetryInsert(func() error {
			_, err := l.db.Exec(ctx, sqlStatement, i.RunId, i.ContainerName, i.ExitCode)
			if err != nil {
				l.metrics.RecordDBError(metrics.DBOperationInsert)
			}
			return err
		})
		if err != nil {
			log.Warnf("Create JobRunContainer run for job run %s, container %s failed with error %+v", i.RunId, i.ContainerName, err)
		}
	}
}

func batchInsert(ctx *context.ArmadaContext, db *pgxpool.Pool, createTmp func(pgx.Tx) error,
	insertTmp func(pgx.Tx) error, copyToDest func(pgx.Tx) error,
) error {
	return pgx.BeginTxFunc(ctx, db, pgx.TxOptions{
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
			return *p == repository.JobFailedOrdinal || *p == repository.JobSucceededOrdinal || *p == repository.JobCancelledOrdinal
		}
	}

	updatesById := make(map[string]*model.UpdateJobInstruction)
	for _, update := range updates {
		existing, ok := updatesById[update.JobId]

		// Unfortunately once a job has reached a terminal state we still get state updates for it e.g. we can get an event to
		// say it's now "running".  We have to throw these away else we'll end up with a zombie job.
		if !ok {
			updatesById[update.JobId] = update
		} else if !isTerminal(existing.State) {
			if update.State != nil {
				existing.State = update.State
			}
			if update.Priority != nil {
				existing.Priority = update.Priority
			}
			if update.Cancelled != nil {
				existing.Cancelled = update.Cancelled
			}
			if update.Duplicate != nil {
				existing.Duplicate = update.Duplicate
			}
			existing.Updated = update.Updated
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
			if update.Succeeded != nil {
				existing.Succeeded = update.Succeeded
			}
			if update.Error != nil {
				existing.Error = update.Error
			}
			if update.PodNumber != nil {
				existing.PodNumber = update.PodNumber
			}
			if update.UnableToSchedule != nil {
				existing.UnableToSchedule = update.UnableToSchedule
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

// filterEventsForTerminalJobs queries the database for any jobs that are in a terminal state and removes them from the list of
// instructions.  This is necessary because Armada will generate event statuses even for jobs that have reached a terminal state
// The proper solution here is to make it so once a job is terminal, no more events are generated for it, but until
// that day we have to manually filter them out here.
// NOTE: this function will retry querying the database for as long as possible in order to determine which jobs are
// in the terminal state.  If, however, the database returns a non-retryable error it will give up and simply not
// filter out any events as the job state is undetermined.
func filterEventsForTerminalJobs(
	ctx *context.ArmadaContext,
	db *pgxpool.Pool,
	instructions []*model.UpdateJobInstruction,
	m *metrics.Metrics,
) []*model.UpdateJobInstruction {
	jobIds := make([]string, len(instructions))
	for i, instruction := range instructions {
		jobIds[i] = instruction.JobId
	}

	rowsRaw, err := withDatabaseRetryQuery(func() (interface{}, error) {
		terminalStates := []int{repository.JobSucceededOrdinal, repository.JobFailedOrdinal, repository.JobCancelledOrdinal}
		return db.Query(ctx, "SELECT DISTINCT job_id FROM JOB where state = any($1) AND job_id = any($2)", terminalStates, jobIds)
	})
	if err != nil {
		m.RecordDBError(metrics.DBOperationRead)
		log.WithError(err).Warnf("Cannot retrieve job state from the database- Terminal jobs may not be filtered out")
		return instructions
	}
	rows := rowsRaw.(pgx.Rows)

	cancelledJobs := make(map[string]bool)
	for rows.Next() {
		jobId := ""
		err := rows.Scan(&jobId)
		if err != nil {
			log.WithError(err).Warnf("Cannot retrieve jobId from row. Terminal job will not be filtered out")
		} else {
			cancelledJobs[jobId] = true
		}
	}

	if len(cancelledJobs) > 0 {
		filtered := make([]*model.UpdateJobInstruction, 0, len(instructions))
		for _, instruction := range instructions {
			if !cancelledJobs[instruction.JobId] {
				filtered = append(filtered, instruction)
			}
		}
		return filtered
	} else {
		return instructions
	}
}

func withDatabaseRetryInsert(executeDb func() error) error {
	_, err := withDatabaseRetryQuery(func() (interface{}, error) {
		return nil, executeDb()
	})
	return err
}

// Executes a database function, retrying until it either succeeds or encounters a non-retryable error
func withDatabaseRetryQuery(executeDb func() (interface{}, error)) (interface{}, error) {
	// TODO: arguably this should come from config
	backOff := 1
	const maxBackoff = 60
	const maxRetries = 10
	numRetries := 0
	var err error = nil
	for attempt := 0; attempt < maxRetries; attempt++ {
		res, err := executeDb()

		if err == nil {
			return res, nil
		}

		if armadaerrors.IsNetworkError(err) || armadaerrors.IsRetryablePostgresError(err) {
			backOff = min(2*backOff, maxBackoff)
			numRetries++
			log.Warnf("Retryable error encountered executing sql, will wait for %d seconds before retrying.  Error was %v", backOff, err)
			time.Sleep(time.Duration(backOff) * time.Second)
		} else {
			// Non retryable error
			return nil, err
		}
	}

	// If we get to here then we've got an error we can't handle.  Panic
	panic(errors.WithStack(&armadaerrors.ErrMaxRetriesExceeded{
		Message:   fmt.Sprintf("Gave up running database query after %d retries", maxRetries),
		LastError: err,
	}))
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
