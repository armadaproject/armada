-- Table: jobs
ALTER TABLE IF EXISTS jobs SET (fillfactor = 70);
ALTER TABLE IF EXISTS jobs ALTER COLUMN groups SET STORAGE EXTERNAL;
ALTER TABLE IF EXISTS jobs ALTER COLUMN submit_message SET STORAGE EXTERNAL;
ALTER TABLE IF EXISTS jobs ALTER COLUMN scheduling_info SET STORAGE EXTERNAL;

-- Table: runs
ALTER TABLE IF EXISTS runs SET (fillfactor = 70);

-- Table: job_run_errors
ALTER TABLE IF EXISTS job_run_errors SET (fillfactor = 70);
ALTER TABLE IF EXISTS job_run_errors ALTER COLUMN error SET STORAGE EXTERNAL;

-- Indexes (fillfactor 80%)
ALTER INDEX IF EXISTS idx_jobs_serial SET (fillfactor = 80);
ALTER INDEX IF EXISTS idx_jobs_queue_jobset SET (fillfactor = 80);
ALTER INDEX IF EXISTS idx_jobs_terminal SET (fillfactor = 80);
ALTER INDEX IF EXISTS idx_runs_serial SET (fillfactor = 80);
ALTER INDEX IF EXISTS idx_runs_job_id SET (fillfactor = 80);
ALTER INDEX IF EXISTS idx_runs_job_set SET (fillfactor = 80);
ALTER INDEX IF EXISTS idx_runs_queue_jobset SET (fillfactor = 80);
ALTER INDEX IF EXISTS idx_runs_filtered SET (fillfactor = 80);
ALTER INDEX IF EXISTS idx_runs_queue_jobset_jobid_filtered SET (fillfactor = 80);
ALTER INDEX IF EXISTS idx_job_run_errors_job_id SET (fillfactor = 80);
