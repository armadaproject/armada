CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_queue_namespace ON job (queue, namespace)
WITH (fillfactor = 80);
