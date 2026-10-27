CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_run_leased ON job_run (leased)
WITH (fillfactor = 80);
