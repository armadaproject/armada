-- Create new index with job_id and terminated (ASC ordered) for better performance
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_queue_job_set_job_id_terminated
ON jobs (queue, job_set, job_id, terminated ASC)
WITH (fillfactor = 60);
