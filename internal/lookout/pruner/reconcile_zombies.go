package pruner

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	log "github.com/armadaproject/armada/internal/common/logging"
)

// reconcileZombiesQuery is built once at package init from the lookout state
// ordinals so the SQL stays the single source of truth for the mapping.
//
// Interpolating these ordinals into the SQL (rather than passing them as bind
// parameters) is safe because they are compile-time constants from
// internal/common/database/lookout, not user input. It also lets PostgreSQL
// infer mapping.new_state as smallint without explicit casts.
var reconcileZombiesQuery = fmt.Sprintf(`
	UPDATE job
	SET state                        = mapping.new_state,
	    last_transition_time         = mapping.finished,
	    last_transition_time_seconds = EXTRACT(EPOCH FROM mapping.finished)::bigint
	FROM (
		SELECT j.job_id        AS job_id,
		       j.queue          AS queue,
		       j.jobset         AS jobset,
		       j.state         AS old_state,
		       j.latest_run_id AS run_id,
		       CASE r.job_run_state
		           WHEN %[1]d THEN %[2]d -- run succeeded -> job succeeded
		           WHEN %[3]d THEN %[4]d -- run failed    -> job failed
		           WHEN %[5]d THEN %[6]d -- run cancelled -> job cancelled
		           WHEN %[7]d THEN %[8]d -- run preempted -> job preempted
		           ELSE j.state          -- defensive: a job_run_state that
		                                 -- passes the IN filter but is not
		                                 -- listed above (e.g. after a future
		                                 -- refactor that lets the lists drift)
		                                 -- becomes a harmless no-op instead
		                                 -- of writing NULL to job.state.
		       END             AS new_state,
		       r.finished      AS finished
		FROM job j
		JOIN job_run r ON r.run_id = j.latest_run_id
		WHERE j.state IN (%[9]d, %[10]d, %[11]d, %[12]d)
		  AND r.job_run_state IN (%[1]d, %[3]d, %[5]d, %[7]d)
		  AND r.finished IS NOT NULL
		  AND r.finished < $1
		LIMIT $2
	) AS mapping
	WHERE job.job_id = mapping.job_id
	  -- Filter out ELSE-branch rows so they don't (a) get last_transition_time
	  -- rewritten unnecessarily, and (b) re-appear in RETURNING and starve the
	  -- outer batch loop's termination condition.
	  AND mapping.new_state != mapping.old_state
	RETURNING job.job_id, mapping.queue, mapping.jobset, mapping.old_state, mapping.new_state, mapping.run_id, mapping.finished`,
	lookout.JobRunSucceededOrdinal, lookout.JobSucceededOrdinal,
	lookout.JobRunFailedOrdinal, lookout.JobFailedOrdinal,
	lookout.JobRunCancelledOrdinal, lookout.JobCancelledOrdinal,
	lookout.JobRunPreemptedOrdinal, lookout.JobPreemptedOrdinal,
	lookout.JobQueuedOrdinal,
	lookout.JobLeasedOrdinal,
	lookout.JobPendingOrdinal,
	lookout.JobRunningOrdinal,
)

// countZombiesWithNullFinishedQuery counts jobs that match the zombie shape
// (non-terminal job.state, latest run in a terminal job_run_state) but whose
// run has no finished timestamp. The reconciler cannot repair these, but they
// should not exist in steady state and so are worth surfacing.
var countZombiesWithNullFinishedQuery = fmt.Sprintf(`
	SELECT COUNT(*)
	FROM job j
	JOIN job_run r ON r.run_id = j.latest_run_id
	WHERE j.state IN (%[5]d, %[6]d, %[7]d, %[8]d)
	  AND r.job_run_state IN (%[1]d, %[2]d, %[3]d, %[4]d)
	  AND r.finished IS NULL`,
	lookout.JobRunSucceededOrdinal,
	lookout.JobRunFailedOrdinal,
	lookout.JobRunCancelledOrdinal,
	lookout.JobRunPreemptedOrdinal,
	lookout.JobQueuedOrdinal,
	lookout.JobLeasedOrdinal,
	lookout.JobPendingOrdinal,
	lookout.JobRunningOrdinal,
)

// ReconcileZombieJobs finds jobs whose state column is non-terminal but whose
// latest run is in a terminal state and finished more than zombieRepairThreshold
// ago, and updates job.state (and last_transition_time) to match the run.
// Returns the number of jobs repaired.
func ReconcileZombieJobs(
	ctx *armadacontext.Context,
	db *pgx.Conn,
	zombieRepairThreshold time.Duration,
	batchLimit int,
	clock clock.Clock,
) (int, error) {
	cutOffTime := clock.Now().Add(-zombieRepairThreshold)
	totalRepaired := 0
	for {
		batchRepaired, err := reconcileZombieBatch(ctx, db, cutOffTime, batchLimit)
		if err != nil {
			return totalRepaired, err
		}
		if batchRepaired == 0 {
			break
		}
		totalRepaired += batchRepaired
	}
	if totalRepaired > 0 {
		// Zombies should not exist in steady state -- their presence
		// indicates a logic bug somewhere upstream of the lookout database.
		// Surface this loudly so it gets noticed and investigated.
		log.Warnf("Reconciled %d zombie job(s) -- this should not happen and indicates an upstream bug", totalRepaired)
	}
	zombiesRepaired.Set(float64(totalRepaired))

	if err := observeZombiesWithNullFinished(ctx, db); err != nil {
		// Diagnostic counting failure: log but do not fail the pruner run.
		log.WithError(err).Warn("failed to count zombie jobs with null run-finished timestamp")
	}

	return totalRepaired, nil
}

// observeZombiesWithNullFinished counts and surfaces zombie jobs that the
// reconciler cannot repair because their latest run has no finished timestamp.
func observeZombiesWithNullFinished(ctx *armadacontext.Context, db *pgx.Conn) error {
	var count int
	if err := db.QueryRow(ctx, countZombiesWithNullFinishedQuery).Scan(&count); err != nil {
		return errors.WithStack(err)
	}
	zombiesSkippedNullFinished.Set(float64(count))
	if count > 0 {
		log.Warnf(
			"Found %d zombie job(s) with no finished timestamp on their latest run -- these were not repaired and should be investigated",
			count,
		)
	}
	return nil
}

// reconcileZombieBatch repairs up to batchLimit zombie jobs in a single
// transaction and returns the number of jobs repaired.
func reconcileZombieBatch(
	ctx *armadacontext.Context,
	db *pgx.Conn,
	cutOffTime time.Time,
	batchLimit int,
) (int, error) {
	type repair struct {
		jobID         string
		queue         string
		jobset        string
		oldState      int
		newState      int
		runID         string
		runFinishedAt time.Time
	}
	var repaired []repair

	err := pgx.BeginTxFunc(ctx, db, pgx.TxOptions{
		IsoLevel:   pgx.ReadCommitted,
		AccessMode: pgx.ReadWrite,
	}, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, reconcileZombiesQuery, cutOffTime, batchLimit)
		if err != nil {
			return errors.WithStack(err)
		}
		defer rows.Close()
		for rows.Next() {
			var r repair
			if err := rows.Scan(&r.jobID, &r.queue, &r.jobset, &r.oldState, &r.newState, &r.runID, &r.runFinishedAt); err != nil {
				return errors.WithStack(err)
			}
			repaired = append(repaired, r)
		}
		return rows.Err()
	})
	if err != nil {
		return 0, errors.Wrap(err, "error reconciling zombie jobs")
	}

	for _, r := range repaired {
		log.Warnf(
			"Repaired zombie job %s (queue=%s jobset=%s): state %s -> %s (latest run %s finished at %s)",
			r.jobID,
			r.queue,
			r.jobset,
			lookout.JobStateMap[r.oldState],
			lookout.JobStateMap[r.newState],
			r.runID,
			r.runFinishedAt.UTC().Format(time.RFC3339),
		)
	}
	return len(repaired), nil
}
