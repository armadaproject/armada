package pruner

import (
	"github.com/armadaproject/armada/internal/common/context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/clock"
)

func PruneDb(ctx *context.ArmadaContext, db *pgx.Conn, keepAfterCompletion time.Duration, batchLimit int, clock clock.Clock) error {
	now := clock.Now()
	cutOffTime := now.Add(-keepAfterCompletion)
	totalJobsToDelete, err := createJobIdsToDeleteTempTable(ctx, db, cutOffTime)
	if err != nil {
		return errors.WithStack(err)
	}
	if totalJobsToDelete == 0 {
		log.Infof("Found no jobs to be deleted. Exiting")
		return nil
	}

	_, err = db.Exec(ctx, "CREATE TEMP TABLE batch (job_id TEXT);")
	if err != nil {
		return errors.WithStack(err)
	}

	jobsDeleted := 0
	keepGoing := true
	for keepGoing {
		batchStart := clock.Now()
		batchSize := 0
		err = pgx.BeginTxFunc(ctx, db, pgx.TxOptions{
			IsoLevel:       pgx.ReadCommitted,
			AccessMode:     pgx.ReadWrite,
			DeferrableMode: pgx.Deferrable,
		}, func(tx pgx.Tx) error {
			batchSize, err = deleteBatch(ctx, tx, batchLimit)
			if err != nil {
				return err
			}
			if batchSize == 0 {
				keepGoing = false
				return nil
			}
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "error deleting batch from postgres")
		}
		batchDuration := clock.Since(batchStart)
		jobsDeleted += batchSize
		log.Infof("Deleted %d jobs in %s. Deleted %d jobs out of %d", batchSize, batchDuration, jobsDeleted, totalJobsToDelete)
	}
	totalTime := clock.Since(now)
	log.Infof("Total jobs deleted: %d, time taken: %v", jobsDeleted, totalTime)
	return nil
}

// Returns total number of jobs to delete
func createJobIdsToDeleteTempTable(ctx *context.ArmadaContext, db *pgx.Conn, cutOffTime time.Time) (int, error) {
	_, err := db.Exec(ctx, `
		CREATE TEMP TABLE job_ids_to_delete AS (
			SELECT job_id FROM job
			WHERE last_transition_time < $1
		)`, cutOffTime)
	if err != nil {
		return -1, errors.WithStack(err)
	}
	totalJobsToDelete := 0
	err = db.QueryRow(ctx, "SELECT COUNT(*) FROM job_ids_to_delete").Scan(&totalJobsToDelete)
	if err != nil {
		return -1, errors.WithStack(err)
	}
	return totalJobsToDelete, nil
}

func deleteBatch(ctx *context.ArmadaContext, tx pgx.Tx, batchLimit int) (int, error) {
	_, err := tx.Exec(ctx, "INSERT INTO batch (job_id) SELECT job_id FROM job_ids_to_delete LIMIT $1;", batchLimit)
	if err != nil {
		return -1, err
	}
	var batchSize int
	err = tx.QueryRow(ctx, "SELECT COUNT(*) FROM batch").Scan(&batchSize)
	if err != nil {
		return -1, err
	}
	if batchSize == 0 {
		return 0, nil
	}
	_, err = tx.Exec(ctx, `
		DELETE FROM job WHERE job_id in (SELECT job_id from batch);
		DELETE FROM job_run WHERE job_id in (SELECT job_id from batch);
		DELETE FROM user_annotation_lookup WHERE job_id in (SELECT job_id from batch);
		DELETE FROM job_ids_to_delete WHERE job_id in (SELECT job_id from batch);
		TRUNCATE TABLE batch;`)
	if err != nil {
		return -1, err
	}
	return batchSize, nil
}
