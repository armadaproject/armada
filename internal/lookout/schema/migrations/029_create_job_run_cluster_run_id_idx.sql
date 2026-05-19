CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_run_cluster_run_id ON job_run (cluster, run_id)
WITH (fillfactor = 80);
