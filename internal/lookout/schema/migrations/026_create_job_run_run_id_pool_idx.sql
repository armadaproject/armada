CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_run_run_id_pool ON job_run (run_id, pool) 
WITH (fillfactor = 80);
