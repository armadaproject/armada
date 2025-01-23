CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_filtered ON runs (executor, succeeded, failed, cancelled, serial);
