CREATE INDEX CONCURRENTLY idx_runs_filtered ON runs (executor, succeeded, failed, cancelled, serial);
