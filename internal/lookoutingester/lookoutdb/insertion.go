package lookoutdb

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/G-Research/armada/internal/lookout/repository"
	"github.com/G-Research/armada/internal/pulsarutils"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/lookoutingester/model"
)

// ProcessUpdates will update the lookout database according to the incoming channel of instructions.  It returns a channel
// containing all the message ids that have been successfully processed.
func ProcessUpdates(ctx context.Context, db *pgxpool.Pool, msgs chan *model.InstructionSet, bufferSize int) chan []*pulsarutils.ConsumerMessageId {
	out := make(chan []*pulsarutils.ConsumerMessageId, bufferSize)
	go func() {
		for msg := range msgs {
			start := time.Now()
			Update(ctx, db, msg)
			taken := time.Now().Sub(start).Milliseconds()
			log.Infof("Inserted %d events in %dms", len(msg.MessageIds), taken)
			out <- msg.MessageIds
		}
		close(out)
	}()
	return out
}

// Update updates the lookout database according to the supplied InstructionSet.
// The updates are applied in the following order:
// * New Job Creations
// * Job Updates, New Job Creations, New User Annotations
// * Job Run Updates, New Job Containers
// In each case we first try to bach insert the rows using the postgres copy protocol.  If this fails then we try a
// slower, serial insert and discard any rows that cannot be inserted.
func Update(ctx context.Context, db *pgxpool.Pool, instructions *model.InstructionSet) {
	// We might have multiple updates for the same job or job run
	// These can be conflated to help performance
	jobsToUpdate := conflateJobUpdates(instructions.JobsToUpdate)
	jobRunsToUpdate := conflateJobRunUpdates(instructions.JobRunsToUpdate)

	// Jobs need to be ingested first as other updates may reference these
	CreateJobs(ctx, db, instructions.JobsToCreate)

	// Now we can job updates, annotations and new job runs
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		UpdateJobs(ctx, db, jobsToUpdate)
	}()
	go func() {
		defer wg.Done()
		CreateJobRuns(ctx, db, instructions.JobRunsToCreate)
	}()
	go func() {
		defer wg.Done()
		CreateUserAnnotations(ctx, db, instructions.UserAnnotationsToCreate)
	}()

	wg.Wait()

	// Finally, we can update the job runs and container exit codes
	wg2 := sync.WaitGroup{}
	wg2.Add(2)
	go func() {
		defer wg2.Done()
		UpdateJobRuns(ctx, db, jobRunsToUpdate)
	}()
	go func() {
		defer wg2.Done()
		CreateJobRunContainers(ctx, db, instructions.JobRunContainersToCreate)
	}()
	wg2.Wait()
}

func CreateJobs(ctx context.Context, db *pgxpool.Pool, instructions []*model.CreateJobInstruction) {
	if len(instructions) == 0 {
		return
	}
	err := CreateJobsBatch(ctx, db, instructions)
	if err != nil {
		log.Warnf("Creating jobs via batch failed, will attempt to insert serially (this might be slow).  Error was %+v", err)
		CreateJobsScalar(ctx, db, instructions)
	}
}

func UpdateJobs(ctx context.Context, db *pgxpool.Pool, instructions []*model.UpdateJobInstruction) {
	if len(instructions) == 0 {
		return
	}
	instructions = filterEventsForCancelledJobs(ctx, db, instructions)
	err := UpdateJobsBatch(ctx, db, instructions)
	if err != nil {
		log.Warnf("Updating jobs via batch failed, will attempt to insert serially (this might be slow).  Error was %+v", err)
		UpdateJobsScalar(ctx, db, instructions)
	}
}

func CreateJobRuns(ctx context.Context, db *pgxpool.Pool, instructions []*model.CreateJobRunInstruction) {
	if len(instructions) == 0 {
		return
	}
	err := CreateJobRunsBatch(ctx, db, instructions)
	if err != nil {
		log.Warnf("Creating job runs via batch failed, will attempt to insert serially (this might be slow).  Error was %+v", err)
		CreateJobRunsScalar(ctx, db, instructions)
	}
}

func UpdateJobRuns(ctx context.Context, db *pgxpool.Pool, instructions []*model.UpdateJobRunInstruction) {
	if len(instructions) == 0 {
		return
	}
	err := UpdateJobRunsBatch(ctx, db, instructions)
	if err != nil {
		log.Warnf("Updating job runs via batch failed, will attempt to insert serially (this might be slow).  Error was %+v", err)
		UpdateJobRunsScalar(ctx, db, instructions)
	}
}

func CreateUserAnnotations(ctx context.Context, db *pgxpool.Pool, instructions []*model.CreateUserAnnotationInstruction) {
	if len(instructions) == 0 {
		return
	}
	err := CreateUserAnnotationsBatch(ctx, db, instructions)
	if err != nil {
		log.Warnf("Creating user annotations via batch failed, will attempt to insert serially (this might be slow).  Error was %+v", err)
		CreateUserAnnotationsScalar(ctx, db, instructions)
	}
}

func CreateJobRunContainers(ctx context.Context, db *pgxpool.Pool, instructions []*model.CreateJobRunContainerInstruction) {
	if len(instructions) == 0 {
		return
	}
	err := CreateJobRunContainersBatch(ctx, db, instructions)
	if err != nil {
		log.Warnf("Creating job run containers via batch failed, will attempt to insert serially (this might be slow).  Error was %+v", err)
		CreateJobRunContainersScalar(ctx, db, instructions)
	}
}

func CreateJobsBatch(ctx context.Context, db *pgxpool.Pool, instructions []*model.CreateJobInstruction) error {
	return withDatabaseRetryInsert(func() error {
		tmpTable := uniqueTableName("job")

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
				  job       jsonb,
                  orig_job_spec bytea,
				  state     smallint,
				  job_updated   timestamp
				) ON COMMIT DROP;`, tmpTable))
			return err
		}

		insertTmp := func(tx pgx.Tx) error {
			_, err := tx.CopyFrom(ctx,
				pgx.Identifier{tmpTable},
				[]string{"job_id", "queue", "owner", "jobset", "priority", "submitted", "job", "orig_job_spec", "state", "job_updated"},
				pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
					return []interface{}{
						instructions[i].JobId,
						instructions[i].Queue,
						instructions[i].Owner,
						instructions[i].JobSet,
						instructions[i].Priority,
						instructions[i].Submitted,
						instructions[i].JobJson,
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
					INSERT INTO job (job_id, queue, owner, jobset, priority, submitted, job, orig_job_spec, state, job_updated) SELECT * from %s
					ON CONFLICT DO NOTHING`, tmpTable),
			)
			return err
		}

		return batchInsert(ctx, db, createTmp, insertTmp, copyToDest)
	})
}

// CreateJobsScalar will insert jobs one by one into the database
func CreateJobsScalar(ctx context.Context, db *pgxpool.Pool, instructions []*model.CreateJobInstruction) {
	sqlStatement := `INSERT INTO job (job_id, queue, owner, jobset, priority, submitted, job, orig_job_spec, state, job_updated)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT DO NOTHING`
	for _, i := range instructions {
		err := withDatabaseRetryInsert(func() error {
			_, err := db.Exec(ctx, sqlStatement, i.JobId, i.Queue, i.Owner, i.JobSet, i.Priority, i.Submitted, i.JobJson, i.JobProto, i.State, i.Updated)
			return err
		})
		if err != nil {
			log.Warnf("Create job for job %s, jobset %s failed with error %+v", i.JobId, i.JobSet, err)
		}
	}
}

func UpdateJobsBatch(ctx context.Context, db *pgxpool.Pool, instructions []*model.UpdateJobInstruction) error {
	return withDatabaseRetryInsert(func() error {
		tmpTable := uniqueTableName("job")

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
			return err
		}

		return batchInsert(ctx, db, createTmp, insertTmp, copyToDest)
	})
}

func UpdateJobsScalar(ctx context.Context, db *pgxpool.Pool, instructions []*model.UpdateJobInstruction) {
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
			_, err := db.Exec(ctx, sqlStatement, i.Priority, i.State, i.Updated, i.Cancelled, i.Duplicate, i.JobId)
			return err
		})
		if err != nil {
			log.Warnf("Updating job %s failed with error %+v", i.JobId, err)
		}
	}
}

func CreateJobRunsBatch(ctx context.Context, db *pgxpool.Pool, instructions []*model.CreateJobRunInstruction) error {
	return withDatabaseRetryInsert(func() error {
		tmpTable := uniqueTableName("job_run")

		createTmp := func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE  %s
				(
				  run_id  varchar(36),
                  job_id  varchar(32),
				  cluster varchar(512),
				  created timestamp
				) ON COMMIT DROP;`, tmpTable))
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
			return err
		}
		return batchInsert(ctx, db, createTmp, insertTmp, copyToDest)
	})
}

func CreateJobRunsScalar(ctx context.Context, db *pgxpool.Pool, instructions []*model.CreateJobRunInstruction) {
	sqlStatement := `INSERT INTO job_run (run_id, job_id, created, cluster)
		 VALUES ($1, $2, $3, $4)
         ON CONFLICT DO NOTHING`
	for _, i := range instructions {
		err := withDatabaseRetryInsert(func() error {
			_, err := db.Exec(ctx, sqlStatement, i.RunId, i.JobId, i.Created, i.Cluster)
			return err
		})
		if err != nil {
			log.Warnf("Create job run for job %s, run %s failed with error %+v", i.JobId, i.RunId, err)
		}
	}
}

func UpdateJobRunsBatch(ctx context.Context, db *pgxpool.Pool, instructions []*model.UpdateJobRunInstruction) error {
	return withDatabaseRetryInsert(func() error {
		tmpTable := uniqueTableName("job_run")

		createTmp := func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s
				(
			      run_id             varchar(36),
			      cluster            varchar(512),
			      node               varchar(512),
			      started            timestamp,
			      finished           timestamp,
			      succeeded          boolean,
			      error              varchar(2048),
			      pod_number         integer,
			      unable_to_schedule boolean
				) ON COMMIT DROP;`, tmpTable))
			return err
		}

		insertTmp := func(tx pgx.Tx) error {
			_, err := tx.CopyFrom(ctx,
				pgx.Identifier{tmpTable},
				[]string{"run_id", "node", "started", "finished", "succeeded", "error", "pod_number", "unable_to_schedule"},
				pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
					return []interface{}{
						instructions[i].RunId,
						instructions[i].Node,
						instructions[i].Started,
						instructions[i].Finished,
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
		                  error = coalesce(tmp.error, job_run.error),
		                  pod_number = coalesce(tmp.pod_number, job_run.pod_number),
		                  unable_to_schedule = coalesce(tmp.unable_to_schedule, job_run.unable_to_schedule)
						FROM %s as tmp where tmp.run_id = job_run.run_id`, tmpTable),
			)
			return err
		}

		return batchInsert(ctx, db, createTmp, insertTmp, copyToDest)
	})
}

func UpdateJobRunsScalar(ctx context.Context, db *pgxpool.Pool, instructions []*model.UpdateJobRunInstruction) {
	sqlStatement := `UPDATE job_run
				SET
				  node = coalesce($1, node),
				  started = coalesce($2, started),
				  finished = coalesce($3, finished),
				  succeeded = coalesce($4, succeeded),
				  error = coalesce($5, error),
				  pod_number = coalesce($6, pod_number),
				  unable_to_schedule = coalesce($7, unable_to_schedule)
				WHERE run_id = $8`
	for _, i := range instructions {
		err := withDatabaseRetryInsert(func() error {
			_, err := db.Exec(ctx, sqlStatement, i.Node, i.Started, i.Finished, i.Succeeded, i.Error, i.PodNumber, i.UnableToSchedule, i.RunId)
			return err
		})
		if err != nil {
			log.Warnf("Updating job run %s failed with error %+v", i.RunId, err)
		}
	}
}

func CreateUserAnnotationsBatch(ctx context.Context, db *pgxpool.Pool, instructions []*model.CreateUserAnnotationInstruction) error {
	return withDatabaseRetryInsert(func() error {
		tmpTable := uniqueTableName("user_annotation_lookup")

		createTmp := func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE  %s
				(
				  job_id  varchar(32),
                  key     varchar(1024),
				  value   varchar(1024)
				) ON COMMIT DROP;`, tmpTable))
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
			return err
		}
		return batchInsert(ctx, db, createTmp, insertTmp, copyToDest)
	})
}

func CreateUserAnnotationsScalar(ctx context.Context, db *pgxpool.Pool, instructions []*model.CreateUserAnnotationInstruction) {
	sqlStatement := `INSERT INTO user_annotation_lookup (job_id, key, value)
		 VALUES ($1, $2, $3) 
         ON CONFLICT DO NOTHING`
	for _, i := range instructions {
		err := withDatabaseRetryInsert(func() error {
			_, err := db.Exec(ctx, sqlStatement, i.JobId, i.Key, i.Value)
			return err
		})
		// TODO- work out what is a retryable error
		if err != nil {
			log.Warnf("Create annotation run for job %s, key %s failed with error %+v", i.JobId, i.Key, err)
		}
	}
}

func CreateJobRunContainersBatch(ctx context.Context, db *pgxpool.Pool, instructions []*model.CreateJobRunContainerInstruction) error {
	return withDatabaseRetryInsert(func() error {
		tmpTable := uniqueTableName("job_run_container")
		createTmp := func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s (
				  run_id  varchar(36),
                  container_name  varchar(512),
				  exit_code integer
				) ON COMMIT DROP;`, tmpTable))
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
			return err
		}

		copyToDest := func(tx pgx.Tx) error {
			_, err := tx.Exec(
				ctx,
				fmt.Sprintf(`
					INSERT INTO job_run_container (run_id, container_name, exit_code) SELECT * from %s
					ON CONFLICT DO NOTHING`, tmpTable))
			return err
		}
		return batchInsert(ctx, db, createTmp, insertTmp, copyToDest)
	})
}

func CreateJobRunContainersScalar(ctx context.Context, db *pgxpool.Pool, instructions []*model.CreateJobRunContainerInstruction) {
	sqlStatement := `INSERT INTO job_run_container (run_id, container_name, exit_code)
		 VALUES ($1, $2, $3)
	     ON CONFLICT DO NOTHING`
	for _, i := range instructions {
		err := withDatabaseRetryInsert(func() error {
			_, err := db.Exec(ctx, sqlStatement, i.RunId, i.ContainerName, i.ExitCode)
			return err
		})
		if err != nil {
			log.Warnf("Create JobRunContainer run for job run %s, container %s failed with error %+v", i.RunId, i.ContainerName, err)
		}
	}
}

func uniqueTableName(table string) string {
	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	return fmt.Sprintf("%s_tmp_%s", table, suffix)
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
	deref := func(p *int32) int32 {
		if p == nil {
			return -1
		} else {
			return *p
		}
	}

	updatesById := make(map[string]*model.UpdateJobInstruction)
	for _, update := range updates {
		existing, ok := updatesById[update.JobId]

		// Unfortunately once a job has been cancelled we still get state updates for it e.g. we can get an event to
		// say it's now "running".  We have to throw these away as cancelled is a terminal state.
		if !ok {
			updatesById[update.JobId] = update
		} else if deref(existing.State) != int32(repository.JobCancelledOrdinal) {
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

// filterEventsForCancelledJobs queries the database for any jobs that are in the cancelled state and removes them from the list of
// instructions.  This is necessary because Armada will generate event stauses even for jobs that have been cancelled
// The proper solution here is to make it so once a job is cancelled, no more events are generated for it, but until
// that day we have to manually filter them out here.
// NOTE: this function will retry querying the database for as long as possible in order to determine which jobs are
// in the cancelling state.  If, however, the database returns a non-retryable error it will give up and simply not
// filter out any events as the job state is undetermined.
func filterEventsForCancelledJobs(ctx context.Context, db *pgxpool.Pool, instructions []*model.UpdateJobInstruction) []*model.UpdateJobInstruction {
	jobIds := make([]string, len(instructions))
	for i, instruction := range instructions {
		jobIds[i] = instruction.JobId
	}

	rowsRaw, err := withDatabaseRetryQuery(func() (interface{}, error) {
		return db.Query(ctx, "SELECT DISTINCT job_id FROM JOB where state = $1 AND job_id = any($2)", repository.JobCancelledOrdinal, jobIds)
	})
	if err != nil {
		log.WithError(err).Warnf("Cannot retrieve job state from the database- Cancelled jobs may not be filtered out")
		return instructions
	}
	rows := rowsRaw.(pgx.Rows)

	cancelledJobs := make(map[string]bool)
	for rows.Next() {
		jobId := ""
		err := rows.Scan(&jobId)
		if err != nil {
			log.WithError(err).Warnf("Cannot retrieve jobId from row. Cancelled job will not be filtered out")
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
