/* Dropping table drops associated indexes, rules, triggers */
DROP TABLE IF EXISTS user_annotation_lookup;

DROP INDEX IF EXISTS idx_job_jobset_last_transition_time_seconds;
DROP INDEX IF EXISTS idx_job_queue_jobset_last_transition_time_seconds;
