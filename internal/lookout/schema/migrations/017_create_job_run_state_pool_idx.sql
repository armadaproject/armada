CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_run_state_pool ON job_run (job_run_state, pool);
