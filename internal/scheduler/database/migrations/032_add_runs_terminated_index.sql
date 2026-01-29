CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_terminated
ON runs (executor, terminated, serial)
WITH (fillfactor = 80);
