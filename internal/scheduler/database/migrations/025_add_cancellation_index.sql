-- Add index to speed up job cancellation queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_queue_job_set_cancelled_succeeded_failed
ON jobs (queue, job_set, cancelled, succeeded, failed)
WITH (fillfactor = 80);
