package database

import (
	"context"
	"github.com/pkg/errors"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/G-Research/armada/pkg/armadaevents"
)

// hasSerial is an Interface for db objects that have serial numbers
type hasSerial interface {
	GetSerial() int64
}

type JobRepository interface {
	// FetchJobUpdates returns all jobs and job runs that have been updated after jobSerial and jobRunSerial respectively
	// These updates are guaranteed to be consistent with each other
	FetchJobUpdates(ctx context.Context, jobSerial int64, jobRunSerial int64) ([]Job, []Run, error)

	// FetchJobRunErrors returns all armadaevents.JobRunErrors for the provided job run ids.  The returned map is
	// keyed by job run id.  Any runs which don't have errors wil be absent from the map.
	FetchJobRunErrors(ctx context.Context, runIds []uuid.UUID) (map[uuid.UUID]*armadaevents.JobRunErrors, error)

	// CountReceivedPartitions returns a count of the number of partition messages present in the database corresponding
	// to the provided groupId.  This is used by the scheduler to determine if the database represents the state of
	// pulsar after a given point in time.
	CountReceivedPartitions(ctx context.Context, groupId uuid.UUID) (uint32, error)
}

// PostgresJobRepository is an implementation of JobRepository that stores its state in postgres
type PostgresJobRepository struct {
	// pool of database connections
	db *pgxpool.Pool
	// maximum number of rows to fetch from postgres in a single query
	batchSize int32
}

// FetchJobRunErrors returns all armadaevents.JobRunErrors for the provided job run ids.  The returned map is
// keyed by job run id.  Any runs which don't have errors wil be absent from the map.
func (r *PostgresJobRepository) FetchJobRunErrors(ctx context.Context, runIds []uuid.UUID) (map[uuid.UUID]*armadaevents.JobRunErrors, error) {
	queries := New(r.db)
	rows, err := queries.SelectRunErrorsById(ctx, runIds)
	if err == nil {
		return nil, errors.WithStack(err)
	}
	jobRunErrors := make([]armadaevents.JobRunErrors, len(rows))
	for i, row := range rows {
		jre := armadaevents.JobRunErrors{}
		err := proto.Unmarshal(row.Error, &jre)
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

// FetchJobUpdates returns all jobs and job runs that have been updated after jobSerial and jobRunSerial respectively
// These updates are guaranteed to be consistent with each other
func (r *PostgresJobRepository) FetchJobUpdates(ctx context.Context, jobSerial int64, jobRunSerial int64) ([]Job, []Run, error) {

	var updatedJobs []Job = nil
	var updatedRuns []Run = nil

	// Use a RepeatableRead transaction here so that we get consistency between jobs and runs
	err := r.db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadOnly,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		var err error = nil
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

		// Fetch runs
		updatedRuns, err = fetch(jobRunSerial, r.batchSize, func(from int64) ([]Run, error) {
			return queries.SelectNewRuns(ctx, SelectNewRunsParams{Serial: from, Limit: r.batchSize})
		})

		return err
	})

	return updatedJobs, updatedRuns, err
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
// Rows are fetches in batches using the supplied fetchBatch function
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
