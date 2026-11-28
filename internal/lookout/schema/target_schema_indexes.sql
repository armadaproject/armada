-- target_schema_indexes.sql
-- Primary key and indexes of the partitioned job table created by
-- target_schema.sql, with the same {{TABLE}} placeholder.
--
-- The PRIMARY KEY is (job_id, state) because PostgreSQL requires the
-- partition key in any unique constraint. job_id alone is still globally
-- unique (ULIDs) -- the state addition is a technical requirement only.
--
-- Because the primary key is partitioned, it cannot serve as a globally
-- ordered access path on job_id: Postgres cannot produce a single ordered
-- scan across partitions from a partitioned index alone. Lookout's default
-- job view filters by queue and sorts by job_id descending, so without
-- idx_{{TABLE}}_queue_job_id below, that query falls back to scanning each
-- partition in job_id order and filtering out non-matching queues row by
-- row -- for a large table this means scanning millions of rows to return
-- a single page of results.

ALTER TABLE {{TABLE}} ADD PRIMARY KEY (job_id, state);

CREATE INDEX idx_{{TABLE}}_queue_last_transition_time_seconds
    ON {{TABLE}} (queue, last_transition_time_seconds)
    WITH (fillfactor = 80);
CREATE INDEX idx_{{TABLE}}_queue_job_id
    ON {{TABLE}} (queue, job_id)
    WITH (fillfactor = 80);
CREATE INDEX idx_{{TABLE}}_queue_jobset_state
    ON {{TABLE}} (queue, jobset, state)
    WITH (fillfactor = 80);
CREATE INDEX idx_{{TABLE}}_state
    ON {{TABLE}} (state)
    WITH (fillfactor = 80);
CREATE INDEX idx_{{TABLE}}_submitted
    ON {{TABLE}} (submitted DESC);
CREATE INDEX idx_{{TABLE}}_jobset_pattern
    ON {{TABLE}} (jobset varchar_pattern_ops)
    WITH (fillfactor = 80);
CREATE INDEX idx_{{TABLE}}_annotations_path
    ON {{TABLE}} USING GIN (annotations jsonb_ops)
    WITH (fastupdate = true, gin_pending_list_limit = 33554432);
CREATE INDEX idx_{{TABLE}}_latest_run_id
    ON {{TABLE}} (latest_run_id)
    WITH (fillfactor = 80);
CREATE INDEX idx_{{TABLE}}_queue_namespace
    ON {{TABLE}} (queue, namespace)
    WITH (fillfactor = 80);
CREATE INDEX idx_{{TABLE}}_ltt_jobid
    ON {{TABLE}} (last_transition_time, job_id)
    WITH (fillfactor = 80);

CREATE INDEX idx_{{TABLE}}_active_queue_jobset
    ON {{TABLE}}_active (queue, jobset)
    WITH (fillfactor = 80);
