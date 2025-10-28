-- Add an index to optimize queries that cancel jobs by queue and job set.
CREATE INDEX CONCURRENTLY idx_jobs_queue_jobset_id (fillfactor = 80) ON jobs (queue, job_set, job_id);
