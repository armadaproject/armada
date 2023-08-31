package repository

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/armadaproject/armada/internal/common/context"

	log "github.com/sirupsen/logrus"
)

const postgresFormat = "2006-01-02 15:04:05.000000"

// DeleteOldJobs deletes jobs from the database that were submitted before cutoff.
// The jobs are deleted in batches with the maximum number of jobs being deleted in
// each batch being given by batchSizeLimit. The tables from which the jobs wil be deleted are:
// * user_annotation_lookup
// * job_run_container
// * job_run
// * job
// This function returns nil if the operation succeeded and an error otherwise. Note that
// For performance reasons we don't use a transaction here and so an error may indicate that
// Some jobs were deleted.
func DeleteOldJobs(db *sql.DB, batchSizeLimit int, cutoff time.Time) error {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// This would be much better done as a proper statement with parameters, but postgres doesn't support
	// parameters if there are multiple statements.
	queryText := fmt.Sprintf(`
				CREATE TEMP TABLE rows_to_delete AS (SELECT job_id FROM job WHERE submitted < '%v' OR submitted IS NULL);
				CREATE TEMP TABLE batch (job_id varchar(32));

				DO
				$do$
					DECLARE
						rows_in_batch integer;
					BEGIN
						LOOP
							INSERT INTO batch(job_id) SELECT job_id FROM rows_to_delete LIMIT '%v';
							GET DIAGNOSTICS rows_in_batch := ROW_COUNT;
							IF rows_in_batch > 0 THEN
								DELETE FROM user_annotation_lookup WHERE job_id in (SELECT job_id from batch);
								DELETE FROM job_run_container WHERE run_id in (SELECT run_id from job_run where job_id in (SELECT job_id from batch));
								DELETE FROM job_run WHERE job_id in (SELECT job_id from batch);
								DELETE FROM job WHERE job_id in (SELECT job_id from batch);
								DELETE FROM rows_to_delete WHERE job_id in (SELECT job_id from batch);
								TRUNCATE TABLE batch;
							ELSE
						EXIT;
							END IF;
						END LOOP;
					END;
				$do$;

				DROP TABLE rows_to_delete;
				DROP TABLE batch;
				`, cutoff.Format(postgresFormat), batchSizeLimit)

	log.Infof("Deleting jobs which haven't changed since cutoff=%v, batch size=%v", cutoff.Format(postgresFormat), batchSizeLimit)
	_, err := db.ExecContext(ctx, queryText)
	if err == nil {
		log.Infof("Deleting jobs finished successfully")
	} else {
		log.Warnf("Deleting jobs failed")
	}
	return err
}
