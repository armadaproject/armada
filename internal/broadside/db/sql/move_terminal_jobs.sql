-- move_terminal_jobs.sql atomically moves a set of jobs from job to job_historical.
--
-- $1 is a text[] array of job_id values to move. The DELETE … RETURNING CTE removes
-- the rows from job and pipes them directly into the INSERT, so no intermediate
-- storage is required and the operation is atomic within a single statement. Rows
-- that are not found in job (e.g. already moved by a concurrent call) are silently
-- skipped because DELETE only returns rows it actually deleted.
--
-- This file is embedded by internal/broadside/db/hotcold.go and executed after each
-- batch's UpdateJobs phase when the HotColdSplit feature toggle is enabled.

WITH moved AS (
    DELETE FROM job WHERE job_id = ANY($1)
    RETURNING
        job_id, queue, owner, jobset,
        cpu, memory, ephemeral_storage, gpu, priority,
        submitted, cancelled, state,
        last_transition_time, last_transition_time_seconds,
        job_spec, duplicate, priority_class, latest_run_id,
        cancel_reason, namespace, annotations, external_job_uri, cancel_user
)
INSERT INTO job_historical (
    job_id, queue, owner, jobset,
    cpu, memory, ephemeral_storage, gpu, priority,
    submitted, cancelled, state,
    last_transition_time, last_transition_time_seconds,
    job_spec, duplicate, priority_class, latest_run_id,
    cancel_reason, namespace, annotations, external_job_uri, cancel_user
)
SELECT * FROM moved;
