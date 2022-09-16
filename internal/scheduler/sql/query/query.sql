-- Jobs
-- name: SelectJobsFromIds :many
SELECT * FROM jobs WHERE job_id = ANY(sqlc.arg(job_ids)::UUID[]);

-- name: SelectQueueJobSetFromId :one
SELECT job_id, queue, job_set FROM jobs where job_id = $1;

-- name: SelectQueueJobSetFromIds :many
SELECT job_id, queue, job_set FROM jobs where job_id = ANY(sqlc.arg(job_ids)::UUID[]);

-- name: SelectNewJobs :many
SELECT * FROM jobs WHERE serial > $1 ORDER BY serial;

-- name: SelectNewActiveJobs :many
SELECT * FROM jobs WHERE serial > $1 AND succeeded = false AND failed = false AND cancelled = false ORDER BY serial;

-- Runs
-- name: SelectUnsentRunsForExecutor :many
SELECT * FROM runs WHERE (executor = $1 AND sent_to_executor = false);

-- name: SelectRunsFromExecutorAndJobs :many
SELECT * FROM runs WHERE (executor = $1 AND job_id = ANY(sqlc.arg(job_ids)::UUID[]));

-- name: SelectNewRunsForJobs :many
SELECT * FROM runs WHERE serial > $1 AND job_id = ANY(sqlc.arg(job_ids)::UUID[]) ORDER BY serial;

-- name: SelectNewRunsForExecutorWithLimit :many
SELECT * FROM runs WHERE (executor = $1 AND sent_to_executor = false) LIMIT $2;

-- name: MarkRunsAsSent :exec
UPDATE runs SET sent_to_executor = true WHERE run_id = ANY(sqlc.arg(run_ids)::UUID[]);

-- name: MarkRunsAsSentByExecutorAndJobId :exec
UPDATE runs SET sent_to_executor = true WHERE executor = $1 AND job_id = ANY(sqlc.arg(job_ids)::UUID[]);

-- NodeInfo
-- name: SelectNewNodeInfo :many
SELECT * FROM nodeinfo WHERE serial > $1 ORDER BY serial;

-- Job priority
-- name: UpdateJobPriorityById :exec
UPDATE jobs SET priority = $1 WHERE job_id = $2;

-- name: UpdateJobPriorityByJobSet :exec
UPDATE jobs SET priority = $1 WHERE job_set = $2;

-- Job cancellation
-- name: MarkJobCancelledById :exec
UPDATE jobs SET cancelled = true WHERE job_id = $1;

-- name: MarkJobsCancelledById :exec
UPDATE jobs SET cancelled = true WHERE job_id = ANY(sqlc.arg(job_ids)::UUID[]);

-- name: MarkJobsCancelledBySet :exec
UPDATE jobs SET cancelled = true WHERE job_set = $1;

-- name: MarkJobsCancelledBySets :exec
UPDATE jobs SET cancelled = true WHERE job_set = ANY(sqlc.arg(job_sets)::text[]);

-- Job succeeded
-- name: MarkJobSucceededById :exec
UPDATE jobs SET succeeded = true WHERE job_id = $1;

-- name: MarkJobsSucceededById :exec
UPDATE jobs SET succeeded = true WHERE job_id = ANY(sqlc.arg(job_ids)::UUID[]);

-- name: MarkJobsSucceededBySet :exec
UPDATE jobs SET succeeded = true WHERE job_set = $1;

-- name: MarkJobsSucceededBySets :exec
UPDATE jobs SET succeeded = true WHERE job_set = ANY(sqlc.arg(job_sets)::text[]);

-- Job failed
-- name: MarkJobFailedById :exec
UPDATE jobs SET failed = true WHERE job_id = $1;

-- name: MarkJobsFailedById :exec
UPDATE jobs SET failed = true WHERE job_id = ANY(sqlc.arg(job_ids)::UUID[]);

-- name: MarkJobsFailedBySet :exec
UPDATE jobs SET failed = true WHERE job_set = $1;

-- name: MarkJobsFailedBySets :exec
UPDATE jobs SET failed = true WHERE job_set = ANY(sqlc.arg(job_sets)::text[]);

-- Job run running
-- name: MarkJobRunRunningById :exec
UPDATE runs SET running = true WHERE run_id = $1;

-- name: MarkJobRunsRunningById :exec
UPDATE runs SET running = true WHERE run_id = ANY(sqlc.arg(run_ids)::UUID[]);

-- Job run failed
-- name: MarkJobRunFailedById :exec
UPDATE runs SET failed = true WHERE run_id = $1;

-- name: MarkJobRunsFailedById :exec
UPDATE runs SET failed = true WHERE run_id = ANY(sqlc.arg(run_ids)::UUID[]);

-- Job run cancelled
-- name: MarkJobRunCancelledByJobId :exec
UPDATE runs SET cancelled = true WHERE job_id = $1;

-- name: MarkJobRunsCancelledByJobId :exec
UPDATE runs SET cancelled = true WHERE job_id = ANY(sqlc.arg(job_ids)::UUID[]);

-- name: MarkJobRunsCancelledBySet :exec
UPDATE runs SET cancelled = true WHERE job_set = $1;

-- name: MarkJobRunsCancelledBySets :exec
UPDATE runs SET cancelled = true WHERE job_set = ANY(sqlc.arg(job_sets)::text[]);

-- Job run succeeded
-- name: MarkJobRunSucceededById :exec
UPDATE runs SET succeeded = true WHERE run_id = $1;

-- name: MarkJobRunsSucceededById :exec
UPDATE runs SET succeeded = true WHERE run_id = ANY(sqlc.arg(run_ids)::UUID[]);

-- Job run assignments
-- name: SelectNewRunAssignments :many
SELECT * FROM job_run_assignments WHERE serial > $1 ORDER BY serial;

-- Leader election
-- Return the row associated with the current leader.
-- If due to a bug several rows are marked as leader, return the most recently modified one.
-- name: SelectLeader :one
SELECT * FROM leaderelection WHERE is_leader = true ORDER BY last_modified DESC LIMIT 1;

-- name: SelectReplicaById :one
SELECT * FROM leaderelection WHERE id = $1 LIMIT 1;

-- name: GetTopicMessageIds :many
SELECT * FROM pulsar WHERE topic = $1;

-- name: UpsertMessageId :exec
INSERT INTO pulsar (topic, ledger_id, entry_id, batch_idx, partition_idx) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (topic, partition_idx) DO UPDATE SET ledger_id = EXCLUDED.ledger_id, entry_id = EXCLUDED.entry_id, batch_idx = EXCLUDED.batch_idx;

-- Job errors
-- name: SelectJobErrorsById :many
SELECT * FROM job_errors WHERE job_id = $1;

-- name: SelectNewJobErrors :many
SELECT * FROM job_errors WHERE serial > $1;

-- Run errors
-- name: SelectRunErrorsById :many
SELECT * FROM job_run_errors WHERE run_id = $1;

-- name: SelectNewRunErrors :many
SELECT * FROM job_run_errors WHERE serial > $1;