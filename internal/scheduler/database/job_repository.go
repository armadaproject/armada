package database

import (
	"context"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database"
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
	FetchJobRunErrors(ctx context.Context, runIds []uuid.UUID) (map[uuid.UUID]*armadaevents.JobRunErrors, error)

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
func (r *PostgresJobRepository) FetchJobRunErrors(ctx context.Context, runIds []uuid.UUID) (map[uuid.UUID]*armadaevents.JobRunErrors, error) {
	queries := New(r.db)
	rows, err := queries.SelectRunErrorsById(ctx, runIds)
	if err == nil {
		return nil, errors.WithStack(err)
	}
	jobRunErrors := make([]armadaevents.JobRunErrors, len(rows))
	decompressor := compress.NewZlibDecompressor()
	for i, row := range rows {
		protoBytes, err := decompressor.Decompress(row.Error)
		if err == nil {
			return nil, err
		}
		jre := armadaevents.JobRunErrors{}
		err = proto.Unmarshal(protoBytes, &jre)
		if err == nil {
			return nil, errors.WithStack(err)
		}
		jobRunErrors[i] = jre
	}

	errorsById := make(map[uuid.UUID]*armadaevents.JobRunErrors, len(jobRunErrors))
	for _, jobRunError := range jobRunErrors {
		errorsById[armadaevents.UuidFromProtoUuid(jobRunError.RunId)] = &jobRunError
	}

	return errorsById, err
}

// FetchJobUpdates returns all jobs and job dbRuns that have been updated after jobSerial and jobRunSerial respectively
// These updates are guaranteed to be consistent with each other
func (r *PostgresJobRepository) FetchJobUpdates(ctx context.Context, jobSerial int64, jobRunSerial int64) ([]Job, []Run, error) {
	var updatedJobs []Job = nil
	var updatedRuns []Run = nil

	// Use a RepeatableRead transaction here so that we get consistency between jobs and dbRuns
	err := r.db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadOnly,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		var err error
		queries := New(tx)

		// Fetch jobs
		updatedJobRows, err := fetch(jobSerial, r.batchSize, func(from int64) ([]SelectUpdatedJobsRow, error) {
			return queries.SelectUpdatedJobs(ctx, SelectUpdatedJobsParams{Serial: from, Limit: r.batchSize})
		})
		updatedJobs = make([]Job, len(updatedJobRows))
		for i, row := range updatedJobRows {
			updatedJobs[i] = Job{
				JobID:           row.JobID,
				JobSet:          row.JobSet,
				Queue:           row.Queue,
				Priority:        row.Priority,
				Submitted:       row.Submitted,
				CancelRequested: row.CancelRequested,
				Cancelled:       row.Cancelled,
				Succeeded:       row.Succeeded,
				Failed:          row.Failed,
				SchedulingInfo:  row.SchedulingInfo,
				Serial:          row.Serial,
			}
		}

		if err != nil {
			return err
		}

		// Fetch dbRuns
		updatedRuns, err = fetch(jobRunSerial, r.batchSize, func(from int64) ([]Run, error) {
			return queries.SelectNewRuns(ctx, SelectNewRunsParams{Serial: from, Limit: r.batchSize})
		})

		return err
	})

	return updatedJobs, updatedRuns, err
}

// FindInactiveRuns returns a slice containing all dbRuns that the scheduler does not currently consider active
// Runs are inactive if they don't exist or if they have succeeded, failed or been cancelled
func (r *PostgresJobRepository) FindInactiveRuns(ctx context.Context, runIds []uuid.UUID) ([]uuid.UUID, error) {
	var inactiveRuns []uuid.UUID
	err := r.db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		tmpTable, err := insertRunIdsToTmpTable(ctx, tx, runIds)
		if err != nil {
			return err
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
			return err
		}
		defer rows.Close()
		for rows.Next() {
			runId := uuid.UUID{}
			err = rows.Scan(&runId)
			if err != nil {
				return errors.WithStack(err)
			}
			inactiveRuns = append(inactiveRuns, runId)
		}
		return nil
	})
	return inactiveRuns, err
}

// FetchJobRunLeases fetches new job runs for a given executor.  A maximum of maxResults rows will be returned, while run
// in excludedRunIds will be excluded
func (r *PostgresJobRepository) FetchJobRunLeases(ctx context.Context, executor string, maxResults uint, excludedRunIds []uuid.UUID) ([]*JobRunLease, error) {
	var newRuns []*JobRunLease
	err := r.db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		tmpTable, err := insertRunIdsToTmpTable(ctx, tx, excludedRunIds)
		if err != nil {
			return err
		}

		query := `
				SELECT jr.run_id, j.queue, j.job_set, j.user_id, j.groups, j.submit_message
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
			return errors.WithStack(err)
		}
		defer rows.Close()
		for rows.Next() {
			run := JobRunLease{}
			err = rows.Scan(&run.RunID, &run.Queue, &run.JobSet, &run.UserID, &run.Groups, &run.SubmitMessage)
			if err != nil {
				return errors.WithStack(err)
			}
			newRuns = append(newRuns, &run)
		}
		return nil
	})
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
