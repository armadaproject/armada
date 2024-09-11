CREATE INDEX IF NOT EXISTS idx_job_queue_pattern_jobset_pattern_state
  ON job USING btree (
    queue COLLATE pg_catalog."default" varchar_pattern_ops ASC NULLS LAST,
    jobset COLLATE pg_catalog."default" varchar_pattern_ops ASC NULLS LAST,
    state ASC NULLS LAST
);

DROP INDEX idx_job_queue_pattern_job_id;

