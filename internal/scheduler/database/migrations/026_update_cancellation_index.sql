-- Drop the old cancellation index
DROP INDEX CONCURRENTLY IF EXISTS idx_queue_job_set_cancelled_succeeded_failed;
-- Drop the old terminal jobs index
DROP INDEX CONCURRENTLY IF EXISTS idx_jobs_terminal;
