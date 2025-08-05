ALTER TABLE job SET (fillfactor = 70);

ALTER INDEX idx_job_queue_last_transition_time_seconds SET (fillfactor = 80);

ALTER INDEX idx_job_queue_jobset_state SET (fillfactor = 80);

ALTER INDEX idx_job_state SET (fillfactor = 80);

DROP INDEX IF EXISTS idx_job_queue_pattern_last_transition_time_seconds;

DROP INDEX IF EXISTS idx_job_jobset_pattern;

DROP INDEX IF EXISTS idx_job_state_last_transition_time_seconds;

DROP INDEX IF EXISTS idx_job_annotations_path;

ALTER TABLE job_run SET (fillfactor = 70);

ALTER INDEX idx_job_run_job_id SET (fillfactor = 80);

ALTER INDEX idx_job_run_node SET (fillfactor = 80);

DROP INDEX IF EXISTS idx_job_run_job_id_node;
