CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_terminal ON jobs (serial, terminated ASC)
WITH (fillfactor = 60);
