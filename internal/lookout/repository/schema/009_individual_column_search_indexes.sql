CREATE INDEX idx_job_queue ON job (queue);

CREATE INDEX idx_job_job_id ON job (job_id);

CREATE INDEX idx_job_owner ON job (owner);

CREATE INDEX idx_job_jobset ON job (jobset);

CREATE INDEX idx_job_state ON job (state);
