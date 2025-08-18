CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_latest_run_id ON job (latest_run_id)
WITH (fillfactor = 80);
