CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_queue_jobset_jobid_filtered ON runs (queue, job_set, job_id, succeeded, failed, cancelled);
