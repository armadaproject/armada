-- jobs are looked up by queue, jobset
CREATE INDEX idx_job_queue_jobset ON job(queue, jobset);

-- ordering of jobs
CREATE INDEX idx_job_submitted ON job(submitted);

-- filtering of running jobs
CREATE INDEX idx_jub_run_finished_null ON job_run(finished) WHERE finished IS NULL;
