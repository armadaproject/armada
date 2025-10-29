-- Drop the old cancellation index
DROP INDEX CONCURRENTLY IF EXISTS idx_queue_job_set_cancelled_succeeded_failed;

-- Add generated column for terminated status (cancelled OR succeeded OR failed)
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS terminated BOOLEAN GENERATED ALWAYS AS (cancelled OR succeeded OR failed) STORED;

-- Create new index with job_id and terminated (DESC ordered) for better performance
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_queue_job_set_job_id_terminated
ON jobs (queue, job_set, job_id, terminated DESC)
WITH (fillfactor = 60);
