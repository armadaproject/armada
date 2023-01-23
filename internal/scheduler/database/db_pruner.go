package database

import (
	ctx "context"
	"time"

	"github.com/jackc/pgtype/pgxtype"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// DeleteOldJobs removes completed jobs (and related runs and errors) from the database if their `lastUpdateTime`
// is more than `keepAfterCompletion` in the past.
// Jobs are deleted in batches across transactions. This means that if this job fails midway through, it still
// may have deleted som jobs.
// The function will run until the supplied context is cancelled.
func DeleteOldJobs(ctx ctx.Context, db pgxtype.Querier, keepAfterCompletion time.Duration) error {
	start := time.Now()
	cutOffTime := time.Now().Add(-keepAfterCompletion)

	// Insert the ids of all jobs we want to delete into a tmp table
	_, err := db.Exec(ctx,
		`CREATE TEMP TABLE rows_to_delete AS (
             SELECT job_id FROM jobs 
			 WHERE last_modified < $1
			 AND (succeeded = TRUE OR failed = TRUE OR cancelled = TRUE))`, cutOffTime)

	if err != nil {
		return errors.WithStack(err)
	}
	var totalJobsToDelete = 0
	err = db.QueryRow(ctx, "SELECT COUNT(*) FROM rows_to_delete").Scan(&totalJobsToDelete)
	if err != nil {
		return errors.WithStack(err)
	}
	if totalJobsToDelete == 0 {
		log.Infof("Fouind no jobs to be deleted. Exiting")
		return nil
	}

	log.Infof("Found %d jobs to be deleted", totalJobsToDelete)

	//  create temp table to hold a batch of results
	_, err = db.Exec(ctx, "CREATE TEMP TABLE batch (job_id TEXT);")
	if err != nil {
		return errors.WithStack(err)
	}
	var jobsDeleted = 0
	for {
		batchStart := time.Now()
		var batchSize = 0
		_, err = db.Exec(ctx, "INSERT INTO batch(job_id) SELECT job_id FROM rows_to_delete LIMIT $1;")
		if err != nil {
			return errors.WithStack(err)
		}

		err := db.QueryRow(ctx, "SELECT COUNT(*) FROM batch").Scan(&batchSize)
		if err != nil {
			return errors.WithStack(err)
		}

		if batchSize < 0 {
			break
		}
		_, err = db.Exec(ctx, "DELETE FROM runs WHERE job_id in (SELECT job_id from batch)")
		if err != nil {
			return errors.WithStack(err)
		}
		_, err = db.Exec(ctx, "DELETE FROM jobs WHERE job_id in (SELECT job_id from batch)")
		if err != nil {
			return errors.WithStack(err)
		}
		taken := time.Now().Sub(batchStart)
		jobsDeleted += batchSize
		log.Infof("Deleted %d jobs in %s.  Deleted %d jobs out of %d", batchSize, taken, jobsDeleted, totalJobsToDelete)
	}
	taken := time.Now().Sub(start)
	log.Infof("Deleted %d jobs in %s", jobsDeleted, taken)
	return nil
}
