CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_awaiting_stop
ON runs (executor)
WHERE stop_request IS NOT NULL AND NOT terminated AND terminated_timestamp IS NULL;
