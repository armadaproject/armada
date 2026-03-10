-- update_and_move_terminal_jobs.sql atomically applies a terminal state update to a
-- set of jobs and moves them from job to job_historical in a single statement.
--
-- This avoids writing terminal states to job, which would violate chk_job_active_state.
--
-- Parameters (all arrays of equal length, one element per job):
--   $1  text[]      job_ids
--   $2  smallint[]  new states (must all be terminal: 4, 5, 6, 7, or 9)
--   $3  timestamp[] new last_transition_time values
--   $4  bigint[]    new last_transition_time_seconds values
--   $5  timestamp[] new cancelled values (NULL for non-cancelled jobs)
--   $6  text[]      new cancel_reason values (NULL for non-cancelled jobs)
--   $7  text[]      new cancel_user values (NULL for non-cancelled jobs)
--
-- Jobs not found in job (e.g. already moved) are silently skipped.
--
-- This file is embedded by internal/broadside/db/hotcold.go and called by
-- updateAndMoveTerminalJobs when the HotColdSplit feature toggle is enabled.

WITH updates AS (
    SELECT *
    FROM UNNEST(
        $1::text[],
        $2::smallint[],
        $3::timestamp[],
        $4::bigint[],
        $5::timestamp[],
        $6::text[],
        $7::text[]
    ) AS t(job_id, new_state, new_ltt, new_ltt_seconds, new_cancelled, new_cancel_reason, new_cancel_user)
),
moved AS (
    DELETE FROM job j
    USING updates u
    WHERE j.job_id = u.job_id
    RETURNING
        j.job_id, j.queue, j.owner, j.jobset,
        j.cpu, j.memory, j.ephemeral_storage, j.gpu, j.priority,
        j.submitted,
        u.new_cancelled                                        AS cancelled,
        u.new_state                                            AS state,
        u.new_ltt                                              AS last_transition_time,
        u.new_ltt_seconds                                      AS last_transition_time_seconds,
        j.job_spec, j.duplicate, j.priority_class, j.latest_run_id,
        COALESCE(u.new_cancel_reason, j.cancel_reason)         AS cancel_reason,
        j.namespace, COALESCE(j.annotations, '{}'::jsonb) AS annotations, j.external_job_uri,
        COALESCE(u.new_cancel_user, j.cancel_user)             AS cancel_user
)
INSERT INTO job_historical (
    job_id, queue, owner, jobset,
    cpu, memory, ephemeral_storage, gpu, priority,
    submitted, cancelled, state,
    last_transition_time, last_transition_time_seconds,
    job_spec, duplicate, priority_class, latest_run_id,
    cancel_reason, namespace, annotations, external_job_uri, cancel_user
)
SELECT
    job_id, queue, owner, jobset,
    cpu, memory, ephemeral_storage, gpu, priority,
    submitted, cancelled, state,
    last_transition_time, last_transition_time_seconds,
    job_spec, duplicate, priority_class, latest_run_id,
    cancel_reason, namespace, annotations, external_job_uri, cancel_user
FROM moved;
