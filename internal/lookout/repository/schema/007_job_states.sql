ALTER TABLE job ADD COLUMN state smallint NULL;

CREATE INDEX idx_job_run_job_id ON job_run (job_id);

CREATE INDEX idx_job_queue_state ON job (queue, state);

CREATE INDEX idx_job_queue_jobset_state ON job (queue, jobset, state);

CREATE OR REPLACE TEMP VIEW run_state_counts AS
SELECT
    run_states.job_id,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE run_state = 1) AS queued,
    COUNT(*) FILTER (WHERE run_state = 2) AS pending,
    COUNT(*) FILTER (WHERE run_state = 3) AS running,
    COUNT(*) FILTER (WHERE run_state = 4) AS succeeded,
    COUNT(*) FILTER (WHERE run_state = 5) AS failed
FROM (
    -- Collect run states for each pod in each job (i.e. the state of each pod)
    SELECT DISTINCT ON (joined_runs.job_id, joined_runs.pod_number)
        joined_runs.job_id,
        joined_runs.pod_number,
        CASE
            WHEN joined_runs.finished IS NOT NULL AND joined_runs.succeeded IS TRUE THEN 4 -- succeeded
            WHEN joined_runs.finished IS NOT NULL AND (joined_runs.succeeded IS FALSE OR joined_runs.succeeded IS NULL) THEN 5 -- failed
            WHEN joined_runs.started IS NOT NULL THEN 3 -- running
            WHEN joined_runs.created IS NOT NULL THEN 2 -- pending
            ELSE 1 -- queued
        END AS run_state
    FROM (
        -- Assume job table is populated
        SELECT
            job.job_id,
            job.submitted,
            job_run.pod_number,
            job_run.created,
            job_run.started,
            job_run.finished,
            job_run.succeeded
        FROM job LEFT JOIN job_run ON job.job_id = job_run.job_id
        WHERE job.cancelled IS NULL AND job.state IS NULL
    ) AS joined_runs
    ORDER BY
        joined_runs.job_id,
        joined_runs.pod_number,
        GREATEST(joined_runs.submitted, joined_runs.created, joined_runs.started, joined_runs.finished) DESC
) AS run_states
GROUP BY run_states.job_id;

-- Queued
UPDATE job
SET state = 1
WHERE job.job_id IN (
    SELECT run_state_counts.job_id
    FROM run_state_counts
    WHERE
        run_state_counts.queued > 0 AND
        run_state_counts.pending = 0 AND
        run_state_counts.running = 0 AND
        run_state_counts.failed = 0
);

-- Pending
UPDATE job
SET state = 2
WHERE job.job_id IN (
    SELECT run_state_counts.job_id
    FROM run_state_counts
    WHERE
        run_state_counts.queued = 0 AND
        run_state_counts.pending > 0 AND
        run_state_counts.failed = 0
);

-- Running
UPDATE job
SET state = 3
WHERE job.job_id IN (
    SELECT run_state_counts.job_id
    FROM run_state_counts
    WHERE
        run_state_counts.queued = 0 AND
        run_state_counts.pending = 0 AND
        run_state_counts.running > 0 AND
        run_state_counts.failed = 0
);

-- Succeeded
UPDATE job
SET state = 4
WHERE job.job_id IN (
    SELECT run_state_counts.job_id
    FROM run_state_counts
    WHERE
        run_state_counts.queued = 0 AND
        run_state_counts.pending = 0 AND
        run_state_counts.running = 0 AND
        run_state_counts.succeeded = run_state_counts.total AND
        run_state_counts.failed = 0
);

-- Failed
UPDATE job
SET state = 5
WHERE job.job_id IN (
    SELECT run_state_counts.job_id
    FROM run_state_counts
    WHERE run_state_counts.failed > 0
);

-- Cancelled
UPDATE job
SET state = 6
WHERE job.job_id IN (
    SELECT job_id
    FROM job
    WHERE cancelled IS NOT NULL
);
