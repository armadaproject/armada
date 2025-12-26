CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_jobset_pattern ON job (jobset varchar_pattern_ops) WITH (fillfactor = 80);
