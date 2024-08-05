-- name: GetJobStates :many
SELECT job_id, state FROM job WHERE job_id = ANY(sqlc.arg(job_ids)::text[]);

-- name: GetJobDetails :many
SELECT job_id, queue, jobset, namespace, state, submitted, cancelled, cancel_reason, last_transition_time, latest_run_id, job_spec FROM job WHERE job_id = ANY(sqlc.arg(job_ids)::text[]);

-- name: GetJobRunsByRunIds :many
SELECT * FROM job_run WHERE run_id = ANY(sqlc.arg(run_ids)::text[]);

-- name: GetJobRunsByJobIds :many
SELECT * FROM job_run WHERE job_id = ANY(sqlc.arg(job_ids)::text[]) order by leased  desc;

