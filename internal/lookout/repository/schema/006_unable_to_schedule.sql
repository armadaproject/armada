ALTER TABLE job_run ADD COLUMN unable_to_schedule bool NULL;

CREATE INDEX idx_job_run_unable_to_schedule_null ON job_run(unable_to_schedule) WHERE unable_to_schedule IS NULL;
