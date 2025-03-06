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
