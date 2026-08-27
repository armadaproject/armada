-- name: GetJobStates :many
SELECT job_id, state FROM job WHERE job_id = ANY(sqlc.arg(job_ids)::text[]);

-- name: GetJobDetails :many
SELECT j.job_id, j.queue, j.jobset, j.namespace, j.state, j.submitted, j.cancelled, j.cancel_reason, j.cancel_user, j.last_transition_time, j.latest_run_id, COALESCE(js.job_spec, j.job_spec) FROM job j left join job_spec js on j.job_id = js.job_id WHERE j.job_id = ANY(sqlc.arg(job_ids)::text[]);

-- name: GetJobRunsByRunIds :many
SELECT * FROM job_run WHERE run_id = ANY(sqlc.arg(run_ids)::text[]);

-- name: GetJobRunsByJobIds :many
SELECT * FROM job_run WHERE job_id = ANY(sqlc.arg(job_ids)::text[]) order by leased  desc;

-- name: GetJobErrorsByJobIds :many
select j.job_id as job_id, coalesce(je.error, jr.error) as error from job j
  left join job_error je on j.job_id = je.job_id
  left join job_run jr on j.latest_run_id = jr.run_id
where j.job_id = ANY(sqlc.arg(job_ids)::text[])
order by j.job_id desc;

-- name: GetJobStatesUsingExternalSystemUri :many
SELECT job_id, state FROM job
WHERE queue=sqlc.arg(queue)::text
  AND jobset = sqlc.arg(jobset)::text
  AND external_job_uri = sqlc.arg(external_job_uri)::text;

-- name: GetActiveQueuesByPool :many
SELECT DISTINCT jr.pool, j.queue FROM job j JOIN job_run jr ON j.job_id = jr.job_id WHERE j.state IN (2, 3, 8) AND jr.job_run_state IN (1, 2, 11) ORDER BY jr.pool, j.queue;

-- name: GetAllRetryPolicies :many
SELECT definition FROM retry_policy ORDER BY name;

-- name: GetRetryPolicy :one
SELECT definition FROM retry_policy WHERE name = sqlc.arg(name)::text;

-- name: CreateRetryPolicy :exec
INSERT INTO retry_policy (name, definition)
VALUES (sqlc.arg(name)::text, sqlc.arg(definition)::bytea)
ON CONFLICT (name) DO UPDATE SET definition = EXCLUDED.definition;

-- name: UpdateRetryPolicy :execrows
UPDATE retry_policy SET definition = sqlc.arg(definition)::bytea WHERE name = sqlc.arg(name)::text;

-- name: DeleteRetryPolicy :exec
DELETE FROM retry_policy WHERE name = sqlc.arg(name)::text;

-- name: GetExistingRetryPolicyNames :many
SELECT name FROM retry_policy WHERE name = ANY(sqlc.arg(names)::text[]);

-- name: DeleteRetryPolicyAttachments :many
DELETE FROM queue_retry_policy WHERE policy_name = sqlc.arg(policy_name)::text RETURNING queue_name;

-- name: GetAllQueueRetryPolicies :many
SELECT queue_name, policy_name FROM queue_retry_policy ORDER BY queue_name, ordinal;

-- name: GetQueueRetryPolicies :many
SELECT policy_name FROM queue_retry_policy WHERE queue_name = sqlc.arg(queue_name)::text ORDER BY ordinal;

-- name: DeleteQueueRetryPolicies :exec
DELETE FROM queue_retry_policy WHERE queue_name = sqlc.arg(queue_name)::text;

-- name: InsertQueueRetryPolicy :exec
-- A name repeated in one queue's list collapses onto the row from its first
-- occurrence, keeping that position's ordinal.
INSERT INTO queue_retry_policy (queue_name, policy_name, ordinal)
VALUES (sqlc.arg(queue_name)::text, sqlc.arg(policy_name)::text, sqlc.arg(ordinal)::int)
ON CONFLICT (queue_name, policy_name) DO NOTHING;
