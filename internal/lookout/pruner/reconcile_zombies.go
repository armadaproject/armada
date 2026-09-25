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
//
// LEASE_RETURNED and LEASE_EXPIRED are mapped to job FAILED rather than left
// unhandled. Ordinarily a lease-returned/expired run is followed by either a
// JobRequeued or a terminal JobErrors event, so the run itself is not a
// reliable terminal signal for the job. But if that follow-up event is lost
// (e.g. dropped by the ingester), the job is stuck showing a non-terminal
// state forever with no other event to correct it. FAILED is a conservative
// choice: it may mislabel a job that was actually still being retried, but it
// stops a permanently stuck job from being reported as active indefinitely.
//
// LEASE_RETURNED/LEASE_EXPIRED use a separate, longer cutoff ($2) than the
// other four run states ($1): unlike the other four, which are unconditionally
// terminal for the job the moment the run reaches them, a lease-returned or
// lease-expired run is normally expected to be followed by a legitimate
// scheduler decision (retry or fail) that can take substantially longer than
// ordinary ingester lag to arrive.
//
// The LEASE_RETURNED/LEASE_EXPIRED branch additionally requires
// job.last_transition_time < run.finished (strictly). A successful requeue
// moves job.state to QUEUED (advancing last_transition_time) without
// changing latest_run_id, which still keeps pointing at the old
// lease-returned/expired run until the next lease is granted. Without this
// check, a job that was legitimately requeued and is simply waiting for its
// next lease -- rather than one whose follow-up event was lost -- would be
// misdetected as a zombie and incorrectly marked FAILED. The comparison must
// be strict rather than <=: a requeue timestamped identically to the
// lease-returned/expired event (e.g. both derived from the same ingested
// batch) still counts as having touched the job.
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
		           WHEN %[1]d THEN %[2]d  -- run succeeded      -> job succeeded
		           WHEN %[3]d THEN %[4]d  -- run failed         -> job failed
		           WHEN %[5]d THEN %[6]d  -- run cancelled      -> job cancelled
		           WHEN %[7]d THEN %[8]d  -- run preempted      -> job preempted
		           WHEN %[13]d THEN %[4]d -- run lease returned -> job failed
		           WHEN %[14]d THEN %[4]d -- run lease expired  -> job failed
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
		  AND r.finished IS NOT NULL
		  AND (
		    (r.job_run_state IN (%[1]d, %[3]d, %[5]d, %[7]d) AND r.finished < $1)
		    OR
		    (
		      r.job_run_state IN (%[13]d, %[14]d)
		      AND r.finished < $2
		      -- A JobRequeued event moves job.state to QUEUED and advances
		      -- last_transition_time, but leaves latest_run_id pointing at the
		      -- now-stale lease-returned/expired run until the next lease is
		      -- granted. Without this check, a job that was legitimately
		      -- requeued and is simply waiting for its next lease would be
		      -- misdetected as a zombie. The comparison is strict (<, not <=)
		      -- because a requeue recorded with the same timestamp as the
		      -- lease-returned/expired event (e.g. both derived from the same
		      -- ingested batch) must still count as "touched": only a
		      -- last_transition_time strictly before finished proves no
		      -- later event has updated the job since the run finished.
		      AND j.last_transition_time < r.finished
		    )
		  )
		LIMIT $3
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
	lookout.JobRunLeaseReturnedOrdinal,
	lookout.JobRunLeaseExpiredOrdinal,
)

// countZombiesWithNullFinishedQuery counts jobs that match the zombie shape
// (non-terminal job.state, latest run in a terminal job_run_state) but whose
// run has no finished timestamp. The reconciler cannot repair these, but they
// should not exist in steady state and so are worth surfacing.
var countZombiesWithNullFinishedQuery = fmt.Sprintf(`
	SELECT COUNT(*)
	FROM job j
	JOIN job_run r ON r.run_id = j.latest_run_id
	WHERE j.state IN (%[7]d, %[8]d, %[9]d, %[10]d)
	  AND r.job_run_state IN (%[1]d, %[2]d, %[3]d, %[4]d, %[5]d, %[6]d)
	  AND r.finished IS NULL`,
	lookout.JobRunSucceededOrdinal,
	lookout.JobRunFailedOrdinal,
	lookout.JobRunCancelledOrdinal,
	lookout.JobRunPreemptedOrdinal,
	lookout.JobRunLeaseReturnedOrdinal,
	lookout.JobRunLeaseExpiredOrdinal,
	lookout.JobQueuedOrdinal,
	lookout.JobLeasedOrdinal,
	lookout.JobPendingOrdinal,
	lookout.JobRunningOrdinal,
)

// ReconcileZombieJobs finds jobs whose state column is non-terminal but whose
// latest run is in a terminal state, or in a lease-returned/lease-expired
// state that never received its expected follow-up event, and updates
// job.state (and last_transition_time) to match the run. The two cases use
// separate grace periods: zombieRepairThreshold for the unconditionally
// terminal run states, and leaseReturnedZombieRepairThreshold (normally much
// longer) for lease-returned/lease-expired, since those are legitimately
// followed by a scheduler retry-or-fail decision that can take a while to
// arrive. A zero threshold disables reconciliation for its run-state group
// independently of the other. Returns the number of jobs repaired.
func ReconcileZombieJobs(
	ctx *armadacontext.Context,
	db *pgx.Conn,
	zombieRepairThreshold time.Duration,
	leaseReturnedZombieRepairThreshold time.Duration,
	batchLimit int,
	clock clock.Clock,
) (int, error) {
	// A zero threshold disables reconciliation for that run-state group. Using
	// the zero time.Time (year 1) as the cutoff, rather than clock.Now(),
	// ensures "r.finished < cutoff" can never match instead of matching
	// everything already finished.
	//
	// clock.Now() is normalized to UTC before use: job_run.finished and
	// job.last_transition_time are always written in UTC (see
	// protoutil.ToStdTime(...).UTC() in the ingester), but clock.Now() is not
	// guaranteed to return a UTC-zoned time.Time -- Postgres's naive
	// "timestamp" columns encode the wall-clock digits of whatever zone the
	// bind parameter is in, not a zone-normalized instant, so comparing a
	// non-UTC cutoff against a UTC-stored value would silently skew every
	// comparison in this file by the process's UTC offset.
	cutOffTime := time.Time{}
	if zombieRepairThreshold > 0 {
		cutOffTime = clock.Now().UTC().Add(-zombieRepairThreshold)
	}
	leaseReturnedCutOffTime := time.Time{}
	if leaseReturnedZombieRepairThreshold > 0 {
		leaseReturnedCutOffTime = clock.Now().UTC().Add(-leaseReturnedZombieRepairThreshold)
	}
	totalRepaired := 0
	for {
		batchRepaired, err := reconcileZombieBatch(ctx, db, cutOffTime, leaseReturnedCutOffTime, batchLimit)
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
	leaseReturnedCutOffTime time.Time,
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
		rows, err := tx.Query(ctx, reconcileZombiesQuery, cutOffTime, leaseReturnedCutOffTime, batchLimit)
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
