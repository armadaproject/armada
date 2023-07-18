package database

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// hasSerial is an Interface for db objects that have serial numbers
type hasSerial interface {
	GetSerial() int64
}

type JobRunLease struct {
	RunID         uuid.UUID
	Queue         string
	JobSet        string
	UserID        string
	Node          string
	Groups        []byte
	SubmitMessage []byte
}

// JobRepository is an interface to be implemented by structs which provide job and run information
type JobRepository interface {
	// FetchJobUpdates returns all jobs and job dbRuns that have been updated after jobSerial and jobRunSerial respectively
	// These updates are guaranteed to be consistent with each other
	FetchJobUpdates(ctx context.Context, jobSerial int64, jobRunSerial int64) ([]Job, []Run, error)

	// FetchJobRunErrors returns all armadaevents.JobRunErrors for the provided job run ids.  The returned map is
	// keyed by job run id.  Any dbRuns which don't have errors wil be absent from the map.
	FetchJobRunErrors(ctx context.Context, runIds []uuid.UUID) (map[uuid.UUID]*armadaevents.Error, error)

	// CountReceivedPartitions returns a count of the number of partition messages present in the database corresponding
	// to the provided groupId.  This is used by the scheduler to determine if the database represents the state of
	// pulsar after a given point in time.
	CountReceivedPartitions(ctx context.Context, groupId uuid.UUID) (uint32, error)

	// FindInactiveRuns returns a slice containing all dbRuns that the scheduler does not currently consider active
	// Runs are inactive if they don't exist or if they have succeeded, failed or been cancelled
	FindInactiveRuns(ctx context.Context, runIds []uuid.UUID) ([]uuid.UUID, error)

	// FetchJobRunLeases fetches new job runs for a given executor.  A maximum of maxResults rows will be returned, while run
	// in excludedRunIds will be excluded
	FetchJobRunLeases(ctx context.Context, executor string, maxResults uint, excludedRunIds []uuid.UUID) ([]*JobRunLease, error)
}

// PostgresJobRepository is an implementation of JobRepository that stores its state in postgres
type PostgresJobRepository struct {
	// pool of database connections
	db *pgxpool.Pool
	// maximum number of rows to fetch from postgres in a single query
	batchSize int32
}

func NewPostgresJobRepository(db *pgxpool.Pool, batchSize int32) *PostgresJobRepository {
	return &PostgresJobRepository{
		db:        db,
		batchSize: batchSize,
	}
}

// FetchJobRunErrors returns all armadaevents.JobRunErrors for the provided job run ids.  The returned map is
// keyed by job run id.  Any dbRuns which don't have errors wil be absent from the map.
func (r *PostgresJobRepository) FetchJobRunErrors(ctx context.Context, runIds []uuid.UUID) (map[uuid.UUID]*armadaevents.Error, error) {
	if len(runIds) == 0 {
		return map[uuid.UUID]*armadaevents.Error{}, nil
	}

	chunks := armadaslices.PartitionToMaxLen(runIds, int(r.batchSize))

	errorsByRunId := make(map[uuid.UUID]*armadaevents.Error, len(runIds))
	decompressor := compress.NewZlibDecompressor()

	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	})
	if err != nil {
		return errorsByRunId, err
	}

	for _, chunk := range chunks {
		tmpTable, err := insertRunIdsToTmpTable(ctx, tx, chunk)
		if err != nil {
			return errorsByRunId, err
		}

		query := `
		SELECT  job_run_errors.run_id, job_run_errors.error
		FROM %s as tmp
		JOIN job_run_errors ON job_run_errors.run_id = tmp.run_id`

		rows, err := tx.Query(ctx, fmt.Sprintf(query, tmpTable))
		if err != nil {
			return errorsByRunId, err
		}
		defer rows.Close()

		for rows.Next() {
			var runId uuid.UUID
			var errorBytes []byte
			err := rows.Scan(&runId, &errorBytes)
			if err != nil {
				return errorsByRunId, errors.WithStack(err)
			}
			jobError, err := protoutil.DecompressAndUnmarshall(errorBytes, &armadaevents.Error{}, decompressor)
			if err != nil {
				return errorsByRunId, errors.WithStack(err)
			}
			errorsByRunId[runId] = jobError
		}
	}

	return errorsByRunId, err
}

// FetchJobUpdates returns all jobs and job dbRuns that have been updated after jobSerial and jobRunSerial respectively
// These updates are guaranteed to be consistent with each other
func (r *PostgresJobRepository) FetchJobUpdates(ctx context.Context, jobSerial int64, jobRunSerial int64) ([]Job, []Run, error) {
	var updatedJobs []Job = nil
	var updatedRuns []Run = nil

	// Use a RepeatableRead transaction here so that we get consistency between jobs and dbRuns
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadOnly,
		DeferrableMode: pgx.Deferrable,
	})
	if err != nil {
		return updatedJobs, updatedRuns, err
	}

	queries := New(tx)

	// Fetch jobs
	updatedJobRows, err := fetch(jobSerial, r.batchSize, func(from int64) ([]SelectUpdatedJobsRow, error) {
		return queries.SelectUpdatedJobs(ctx, SelectUpdatedJobsParams{Serial: from, Limit: r.batchSize})
	})
	updatedJobs = make([]Job, len(updatedJobRows))
	for i, row := range updatedJobRows {
		updatedJobs[i] = Job{
			JobID:                   row.JobID,
			JobSet:                  row.JobSet,
			Queue:                   row.Queue,
			Priority:                row.Priority,
			Submitted:               row.Submitted,
			Queued:                  row.Queued,
			QueuedVersion:           row.QueuedVersion,
			CancelRequested:         row.CancelRequested,
			Cancelled:               row.Cancelled,
			CancelByJobsetRequested: row.CancelByJobsetRequested,
			Succeeded:               row.Succeeded,
			Failed:                  row.Failed,
			SchedulingInfo:          row.SchedulingInfo,
			SchedulingInfoVersion:   row.SchedulingInfoVersion,
			Serial:                  row.Serial,
		}
	}
	if err != nil {
		return updatedJobs, updatedRuns, err
	}

	// Fetch dbRuns
	updatedRuns, err = fetch(jobRunSerial, r.batchSize, func(from int64) ([]Run, error) {
		return queries.SelectNewRuns(ctx, SelectNewRunsParams{Serial: from, Limit: r.batchSize})
	})

	return updatedJobs, updatedRuns, err
}

// FindInactiveRuns returns a slice containing all dbRuns that the scheduler does not currently consider active
// Runs are inactive if they don't exist or if they have succeeded, failed or been cancelled
func (r *PostgresJobRepository) FindInactiveRuns(ctx context.Context, runIds []uuid.UUID) ([]uuid.UUID, error) {
	var inactiveRuns []uuid.UUID
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	})
	if err != nil {
		return inactiveRuns, err
	}
	tmpTable, err := insertRunIdsToTmpTable(ctx, tx, runIds)
	if err != nil {
		return inactiveRuns, err
	}

	query := `
		SELECT tmp.run_id
		FROM %s as tmp
		LEFT JOIN runs ON (tmp.run_id = runs.run_id)
		WHERE runs.run_id IS NULL
		OR runs.succeeded = true
 		OR runs.failed = true
		OR runs.cancelled = true;`

	rows, err := tx.Query(ctx, fmt.Sprintf(query, tmpTable))
	if err != nil {
		return inactiveRuns, err
	}
	defer rows.Close()
	for rows.Next() {
		runId := uuid.UUID{}
		err = rows.Scan(&runId)
		if err != nil {
			return inactiveRuns, errors.WithStack(err)
		}
		inactiveRuns = append(inactiveRuns, runId)
	}
	return inactiveRuns, err
}

// FetchJobRunLeases fetches new job runs for a given executor.  A maximum of maxResults rows will be returned, while run
// in excludedRunIds will be excluded
func (r *PostgresJobRepository) FetchJobRunLeases(ctx context.Context, executor string, maxResults uint, excludedRunIds []uuid.UUID) ([]*JobRunLease, error) {
	var newRuns []*JobRunLease
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	})
	if err != nil {
		return newRuns, err
	}

	tmpTable, err := insertRunIdsToTmpTable(ctx, tx, excludedRunIds)
	if err != nil {
		return newRuns, err
	}

	query := `
				SELECT jr.run_id, jr.node, j.queue, j.job_set, j.user_id, j.groups, j.submit_message
				FROM runs jr
				LEFT JOIN %s as tmp ON (tmp.run_id = jr.run_id)
			    JOIN jobs j
			    ON jr.job_id = j.job_id
				WHERE jr.executor = $1
			    AND tmp.run_id IS NULL
				AND jr.succeeded = false
				AND jr.failed = false
				AND jr.cancelled = false
				LIMIT %d;
`
	rows, err := tx.Query(ctx, fmt.Sprintf(query, tmpTable, maxResults), executor)
	if err != nil {
		return newRuns, errors.WithStack(err)
	}
	defer rows.Close()
	for rows.Next() {
		run := JobRunLease{}
		err = rows.Scan(&run.RunID, &run.Node, &run.Queue, &run.JobSet, &run.UserID, &run.Groups, &run.SubmitMessage)
		if err != nil {
			return newRuns, errors.WithStack(err)
		}
		newRuns = append(newRuns, &run)
	}
	return newRuns, err
}

// CountReceivedPartitions returns a count of the number of partition messages present in the database corresponding
// to the provided groupId.  This is used by the scheduler to determine if the database represents the state of
// pulsar after a given point in time.
func (r *PostgresJobRepository) CountReceivedPartitions(ctx context.Context, groupId uuid.UUID) (uint32, error) {
	queries := New(r.db)
	count, err := queries.CountGroup(ctx, groupId)
	if err != nil {
		return 0, err
	}
	return uint32(count), nil
}

// fetch gets all rows from the database with a serial greater than from.
// Rows are fetched in batches using the supplied fetchBatch function
func fetch[T hasSerial](from int64, batchSize int32, fetchBatch func(int64) ([]T, error)) ([]T, error) {
	values := make([]T, 0)
	for {
		batch, err := fetchBatch(from)
		if err != nil {
			return nil, err
		}
		values = append(values, batch...)
		if len(batch) < int(batchSize) {
			break
		}
		from = batch[len(batch)-1].GetSerial()
	}
	return values, nil
}

// Insert all run ids into a tmp table.  The name of the table is returned
func insertRunIdsToTmpTable(ctx context.Context, tx pgx.Tx, runIds []uuid.UUID) (string, error) {
	tmpTable := database.UniqueTableName("job_runs")

	_, err := tx.Exec(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s (run_id  uuid) ON COMMIT DROP", tmpTable))
	if err != nil {
		return "", errors.WithStack(err)
	}
	_, err = tx.CopyFrom(ctx,
		pgx.Identifier{tmpTable},
		[]string{"run_id"},
		pgx.CopyFromSlice(len(runIds), func(i int) ([]interface{}, error) {
			return []interface{}{
				runIds[i],
			}, nil
		}),
	)
	return tmpTable, err
}
