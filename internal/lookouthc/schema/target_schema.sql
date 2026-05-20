-- target_schema.sql
-- Target partitioned shape for the job table in the experimental hot-cold
-- phase.
--
-- Placeholder: {{TABLE}} is replaced at render time with the parent table
-- name (typically 'job_new' during conversion, 'job' for a from-scratch
-- setup).
--
-- Shape: LIST-partitioned on state:
--   active partition:     states 1 (Queued), 2 (Pending), 3 (Running), 8 (Leased)
--   terminated partition: states 4 (Succeeded), 5 (Failed), 6 (Cancelled),
--                         7 (Preempted), 9 (Rejected)
--
-- PostgreSQL automatically moves rows between partitions when state is
-- updated to a value in a different partition.
--
-- The PRIMARY KEY is (job_id, state) because PostgreSQL requires the
-- partition key in any unique constraint. job_id alone is still globally
-- unique (ULIDs) -- the state addition is a technical requirement only.

CREATE TABLE {{TABLE}} (
    job_id                       varchar(32)   NOT NULL,
    queue                        varchar(512)  NOT NULL,
    owner                        varchar(512)  NOT NULL,
    jobset                       varchar(1024) NOT NULL,
    cpu                          bigint        NOT NULL,
    memory                       bigint        NOT NULL,
    ephemeral_storage            bigint        NOT NULL,
    gpu                          bigint        NOT NULL,
    priority                     bigint        NOT NULL,
    submitted                    timestamp     NOT NULL,
    cancelled                    timestamp     NULL,
    state                        smallint      NOT NULL,
    last_transition_time         timestamp     NOT NULL,
    last_transition_time_seconds bigint        NOT NULL,
    job_spec                     bytea         NULL,
    duplicate                    bool          NOT NULL DEFAULT false,
    priority_class               varchar(63)   NULL,
    latest_run_id                varchar(36)   NULL,
    cancel_reason                varchar(512)  NULL,
    namespace                    varchar(512)  NULL,
    annotations                  jsonb         NOT NULL,
    external_job_uri             varchar(1024) NULL,
    cancel_user                  varchar(512)  NULL,
    PRIMARY KEY (job_id, state)
) PARTITION BY LIST (state);

CREATE TABLE {{TABLE}}_active PARTITION OF {{TABLE}}
    FOR VALUES IN (1, 2, 3, 8)
    WITH (fillfactor = 70);

CREATE TABLE {{TABLE}}_terminated PARTITION OF {{TABLE}}
    FOR VALUES IN (4, 5, 6, 7, 9)
    WITH (fillfactor = 70);

ALTER TABLE {{TABLE}}_active ALTER COLUMN job_spec SET STORAGE EXTERNAL;
ALTER TABLE {{TABLE}}_terminated ALTER COLUMN job_spec SET STORAGE EXTERNAL;

CREATE INDEX idx_{{TABLE}}_queue_last_transition_time_seconds
    ON {{TABLE}} (queue, last_transition_time_seconds)
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
