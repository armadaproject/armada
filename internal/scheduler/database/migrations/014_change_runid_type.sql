ALTER TABLE runs ALTER COLUMN run_id TYPE text USING run_id::text;
ALTER TABLE job_run_errors ALTER COLUMN run_id TYPE text USING run_id::text;
