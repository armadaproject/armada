ALTER TABLE job_run ADD COLUMN pool TEXT;

CREATE INDEX CONCURRENTLY idx_job_run_state_pool ON job_run (job_run_state, pool);
