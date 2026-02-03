CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_queue_jobset_jobid_terminated
ON runs (queue, job_set, job_id, terminated)
WITH (fillfactor = 80);
