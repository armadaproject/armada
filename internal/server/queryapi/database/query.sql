-- name: GetJobStates :many
SELECT job_id, state FROM job WHERE job_id = ANY(sqlc.arg(job_ids)::text[]);

-- name: GetJobDetails :many
SELECT j.job_id, j.queue, j.jobset, j.namespace, j.state, j.submitted, j.cancelled, j.cancel_reason, j.last_transition_time, j.latest_run_id, COALESCE(js.job_spec, j.job_spec) FROM job j left join job_spec js on j.job_id = js.job_id WHERE j.job_id = ANY(sqlc.arg(job_ids)::text[]);

-- name: GetJobRunsByRunIds :many
SELECT * FROM job_run WHERE run_id = ANY(sqlc.arg(run_ids)::text[]);

-- name: GetJobRunsByJobIds :many
SELECT * FROM job_run WHERE job_id = ANY(sqlc.arg(job_ids)::text[]) order by leased  desc;

