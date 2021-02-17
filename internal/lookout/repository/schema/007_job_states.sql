ALTER TABLE job
    ADD COLUMN last_event timestamp NULL,
    ADD COLUMN state      smallint  NULL;

CREATE INDEX idx_job_queue_state ON job(queue, state);

CREATE INDEX idx_job_queue_jobset_state ON job(queue, jobset, state);
