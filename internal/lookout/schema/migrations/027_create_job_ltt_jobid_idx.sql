CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_ltt_jobid ON job (last_transition_time, job_id)
WITH (fillfactor = '80');
