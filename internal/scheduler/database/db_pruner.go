package database

import (
	ctx "context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/clock"
)

// PruneDb removes completed jobs (and related runs and errors) from the database if their `lastUpdateTime`
// is more than `keepAfterCompletion` in the past.
// Jobs are deleted in batches across transactions. This means that if this job fails midway through, it still
// may have deleted some jobs.
// The function will run until the supplied context is cancelled.
func PruneDb(ctx ctx.Context, db *pgx.Conn, batchLimit int, keepAfterCompletion time.Duration, clock clock.Clock) error {
	start := time.Now()
	cutOffTime := clock.Now().Add(-keepAfterCompletion)

	// First delete the partition messages. This table shouldn't be big so no need for anything fancy
	err := New(db).DeleteOldMarkers(ctx, cutOffTime)
	if err != nil {
		return errors.Wrapf(err, "Error deleting markers")
	}

	// Insert the ids of all jobs we want to delete into a tmp table
	_, err = db.Exec(ctx,
		`CREATE TEMP TABLE rows_to_delete AS (
             SELECT job_id FROM jobs 
			 WHERE last_modified < $1
			 AND (succeeded = TRUE OR failed = TRUE OR cancelled = TRUE))`, cutOffTime)
	if err != nil {
		return errors.WithStack(err)
	}
	totalJobsToDelete := 0
	err = db.QueryRow(ctx, "SELECT COUNT(*) FROM rows_to_delete").Scan(&totalJobsToDelete)
	if err != nil {
		return errors.WithStack(err)
	}
	if totalJobsToDelete == 0 {
		log.Infof("Found no jobs to be deleted. Exiting")
		return nil
	}

	log.Infof("Found %d jobs to be deleted", totalJobsToDelete)

	//  create temp table to hold a batch of results
	_, err = db.Exec(ctx, "CREATE TEMP TABLE batch (job_id TEXT);")
	if err != nil {
		return errors.WithStack(err)
	}
	jobsDeleted := 0
	for {
		batchStart := time.Now()
		batchSize := 0
		tx, err := db.BeginTx(ctx, pgx.TxOptions{
			IsoLevel:       pgx.ReadCommitted,
			AccessMode:     pgx.ReadWrite,
			DeferrableMode: pgx.Deferrable,
		})
		if err != nil {
			return errors.Wrapf(err, "Error deleting batch from postgres")
		}

		// insert into the batch table
		_, err = tx.Exec(ctx, "INSERT INTO batch(job_id) SELECT job_id FROM rows_to_delete LIMIT $1;", batchLimit)
		if err != nil {
			return errors.Wrapf(err, "Error deleting batch from postgres")
		}
		err = tx.QueryRow(ctx, "SELECT COUNT(*) FROM batch").Scan(&batchSize)
		if err != nil {
			return errors.Wrapf(err, "Error deleting batch from postgres")
		}

		if batchSize == 0 {
			// nothing more to delete
			break
		}

		// Delete everything that's present in the batch table
		// Do this all in one call so as to be more terse with the syntax
		_, err = tx.Exec(ctx, `
						DELETE FROM runs WHERE job_id in (SELECT job_id from batch);
						DELETE FROM jobs WHERE job_id in (SELECT job_id from batch);
						DELETE FROM job_run_errors WHERE job_id in (SELECT job_id from batch);
						DELETE FROM rows_to_delete WHERE job_id in (SELECT job_id from batch);
						TRUNCATE TABLE batch;`)
		if err != nil {
			return errors.Wrapf(err, "Error deleting batch from postgres")
		}

		taken := time.Now().Sub(batchStart)
		jobsDeleted += batchSize
		log.Infof("Deleted %d jobs in %s.  Deleted %d jobs out of %d", batchSize, taken, jobsDeleted, totalJobsToDelete)
	}
	taken := time.Now().Sub(start)
	log.Infof("Deleted %d jobs in %s", jobsDeleted, taken)
	return nil
}
