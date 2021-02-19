ALTER TABLE job ADD COLUMN state smallint NULL;

CREATE INDEX idx_job_run_job_id ON job_run(job_id);

CREATE INDEX idx_job_queue_state ON job(queue, state);

CREATE INDEX idx_job_queue_jobset_state ON job(queue, jobset, state);
