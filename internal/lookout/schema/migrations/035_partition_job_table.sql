-- Converts the job table from a plain heap to a LIST-partitioned table on the
-- state column, matching the target shape in
-- internal/lookouthc/schema/target_schema.sql.
--
-- Partitions:
--   job_active:     states 1 (Queued), 2 (Pending), 3 (Running), 8 (Leased)
--   job_terminated: states 4 (Succeeded), 5 (Failed), 6 (Cancelled),
--                   7 (Preempted), 9 (Rejected)
--
-- The PRIMARY KEY changes from (job_id) to (job_id, state) because PostgreSQL
-- requires the partition key in any unique constraint. job_id alone remains
-- globally unique via ULID; the extra column is a technical requirement only.
--
-- Also adds cancel_reason varchar(512) NULL to match target_schema.sql.
--
-- WARNING: This migration rewrites the entire job table and is NOT safe to run
-- against a live, write-active database. Run during a maintenance window or
-- with the ingestor paused.

BEGIN;

-- Step 1: Rename the existing table so its indexes are out of the way.
ALTER TABLE job RENAME TO job_old;

-- Step 2: Create the new partitioned parent table.
CREATE TABLE job (
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

CREATE TABLE job_active PARTITION OF job
    FOR VALUES IN (1, 2, 3, 8)
    WITH (fillfactor = 70);

CREATE TABLE job_terminated PARTITION OF job
    FOR VALUES IN (4, 5, 6, 7, 9)
    WITH (fillfactor = 70);

ALTER TABLE job_active ALTER COLUMN job_spec SET STORAGE EXTERNAL;
ALTER TABLE job_terminated ALTER COLUMN job_spec SET STORAGE EXTERNAL;

ALTER TABLE job_active ADD CONSTRAINT job_active_job_id_unique UNIQUE (job_id);
ALTER TABLE job_terminated ADD CONSTRAINT job_terminated_job_id_unique UNIQUE (job_id);

-- Step 3: Copy all rows.
INSERT INTO job (
    job_id, queue, owner, jobset, cpu, memory, ephemeral_storage, gpu,
    priority, submitted, cancelled, state, last_transition_time,
    last_transition_time_seconds, job_spec, duplicate, priority_class,
    latest_run_id, cancel_reason, namespace, annotations, external_job_uri,
    cancel_user
)
SELECT
    job_id, queue, owner, jobset, cpu, memory, ephemeral_storage, gpu,
    priority, submitted, cancelled, state, last_transition_time,
    last_transition_time_seconds, job_spec, duplicate, priority_class,
    latest_run_id, cancel_reason, namespace, annotations, external_job_uri,
    cancel_user
FROM job_old;

-- Step 4: Drop the old table, which also removes its associated indexes.
DROP TABLE job_old;

-- Step 5: Rebuild all indexes on the new partitioned table. PostgreSQL
-- automatically maintains per-partition index segments for parent-level indexes.
CREATE INDEX idx_job_queue_last_transition_time_seconds
    ON job (queue, last_transition_time_seconds)
    WITH (fillfactor = 80);
CREATE INDEX idx_job_queue_jobset_state
    ON job (queue, jobset, state)
    WITH (fillfactor = 80);
CREATE INDEX idx_job_state
    ON job (state)
    WITH (fillfactor = 80);
CREATE INDEX idx_job_submitted
    ON job (submitted DESC);
CREATE INDEX idx_job_jobset_pattern
    ON job (jobset varchar_pattern_ops)
    WITH (fillfactor = 80);
CREATE INDEX idx_job_annotations_path
    ON job USING GIN (annotations jsonb_ops)
    WITH (fastupdate = true, gin_pending_list_limit = 33554432);
CREATE INDEX idx_job_latest_run_id
    ON job (latest_run_id)
    WITH (fillfactor = 80);
CREATE INDEX idx_job_queue_namespace
    ON job (queue, namespace)
    WITH (fillfactor = 80);
CREATE INDEX idx_job_ltt_jobid
    ON job (last_transition_time, job_id)
    WITH (fillfactor = 80);

-- Partition-specific: replaces the partial index from migration 030.
-- It now lives on job_active directly rather than as a partial
-- WHERE state IN (1, 2, 3, 8) predicate on the parent table.
CREATE INDEX idx_job_active_queue_jobset
    ON job_active (queue, jobset)
    WITH (fillfactor = 80);

COMMIT;
