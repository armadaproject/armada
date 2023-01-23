-- name: SelectNewJobs :many
SELECT * FROM jobs WHERE serial > $1 ORDER BY serial LIMIT $2;

-- name: SelectAllJobIds :many
SELECT job_id FROM jobs;

-- name: SelectUpdatedJobs :many
SELECT job_id, job_set, queue, priority, submitted, cancel_requested, cancelled, succeeded, failed, scheduling_info, serial FROM jobs WHERE serial > $1 ORDER BY serial LIMIT $2;

-- name: UpdateJobPriorityByJobSet :exec
UPDATE jobs SET priority = $1 WHERE job_set = $2;

-- name: MarkJobsCancelledBySets :exec
UPDATE jobs SET cancelled = true WHERE job_set = ANY(sqlc.arg(job_sets)::text[]);

-- name: MarkJobsSucceededById :exec
UPDATE jobs SET succeeded = true WHERE job_id = ANY(sqlc.arg(job_ids)::text[]);

-- name: MarkJobsCancelledById :exec
UPDATE jobs SET cancelled = true WHERE job_id = ANY(sqlc.arg(job_ids)::text[]);

-- name: MarkJobsFailedById :exec
UPDATE jobs SET failed = true WHERE job_id = ANY(sqlc.arg(job_ids)::text[]);

-- name: UpdateJobPriorityById :exec
UPDATE jobs SET priority = $1 WHERE job_id = $2;

-- name: SelectNewRuns :many
SELECT * FROM runs WHERE serial > $1 ORDER BY serial LIMIT $2;

-- name: SelectAllRunIds :many
SELECT run_id FROM runs;

-- name: SelectNewRunsForJobs :many
SELECT * FROM runs WHERE serial > $1 AND job_id = ANY(sqlc.arg(job_ids)::text[]) ORDER BY serial;

-- name: MarkJobRunsCancelledBySets :exec
UPDATE runs SET cancelled = true WHERE job_set = ANY(sqlc.arg(job_sets)::text[]);

-- name: MarkJobRunsCancelledByJobId :exec
UPDATE runs SET cancelled = true WHERE job_id = ANY(sqlc.arg(job_ids)::text[]);

-- name: MarkJobRunsSucceededById :exec
UPDATE runs SET succeeded = true WHERE run_id = ANY(sqlc.arg(run_ids)::UUID[]);

-- name: MarkJobRunsFailedById :exec
UPDATE runs SET failed = true WHERE run_id = ANY(sqlc.arg(run_ids)::UUID[]);

-- name: MarkJobRunsRunningById :exec
UPDATE runs SET running = true WHERE run_id = ANY(sqlc.arg(run_ids)::UUID[]);

-- name: SelectJobsForExecutor :many
SELECT jr.run_id, j.queue, j.job_set, j.user_id, j.groups, j.submit_message
FROM runs jr
         JOIN jobs j
              ON jr.job_id = j.job_id
WHERE jr.executor = $1
  AND jr.run_id NOT IN (sqlc.arg(run_ids)::UUID[])
  AND jr.succeeded = false AND jr.failed = false AND jr.cancelled = false;

-- name: FindActiveRuns :many
SELECT run_id FROM runs WHERE run_id = ANY(sqlc.arg(run_ids)::UUID[])
                         AND (succeeded = false AND failed = false AND cancelled = false);

-- name: CountGroup :one
SELECT COUNT(*) FROM markers WHERE group_id= $1;

-- Run errors
-- name: SelectRunErrorsById :many
SELECT * FROM job_run_errors WHERE run_id = ANY(sqlc.arg(run_ids)::UUID[]);