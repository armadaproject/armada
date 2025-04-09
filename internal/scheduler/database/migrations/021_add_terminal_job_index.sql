CREATE INDEX IF NOT EXISTS idx_jobs_terminal ON jobs (serial, cancelled, succeeded, failed);
