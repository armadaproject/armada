CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_run_run_id_node ON job_run (run_id, node)
WITH (fillfactor = 80);
