-- name: GetJobState :many
SELECT state FROM job WHERE job_id = $1;
