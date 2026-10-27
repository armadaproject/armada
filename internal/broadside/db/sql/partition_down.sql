-- partition_down.sql reverts the partition-by-submitted migration.
--
-- It replaces the partitioned job table with an unpartitioned one and
-- removes the submitted column from child tables. No data is preserved:
-- the caller truncates all tables before running this.
--
-- This file is embedded by internal/broadside/db/partition.go and executed
-- by TearDown when the PartitionBySubmitted feature toggle is enabled.

BEGIN;

-- Drop the partitioned job table (cascades to all partitions)
DROP TABLE IF EXISTS job CASCADE;

-- Recreate unpartitioned job table matching the Lookout schema
CREATE TABLE job (
    job_id                       varchar(32)   NOT NULL PRIMARY KEY,
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
    annotations                  jsonb         NOT NULL DEFAULT '{}'::jsonb,
    external_job_uri             varchar(1024) NULL,
    cancel_user                  varchar(512)  NULL
);

ALTER TABLE job ALTER COLUMN job_spec SET STORAGE EXTERNAL;

CREATE INDEX idx_job_queue ON job (queue);

CREATE INDEX idx_job_queue_last_transition_time_seconds ON job (
    queue,
    last_transition_time_seconds
);

CREATE INDEX idx_job_jobset_last_transition_time_seconds ON job (
    jobset,
    last_transition_time_seconds
);

CREATE INDEX idx_job_queue_jobset_last_transition_time_seconds ON job (
    queue,
    jobset,
    last_transition_time_seconds
);

CREATE INDEX idx_job_queue_jobset_state ON job (queue, jobset, state);

CREATE INDEX idx_job_state ON job (state);

CREATE INDEX idx_job_jobset_pattern ON job (jobset varchar_pattern_ops) WITH (fillfactor = 80);

CREATE INDEX idx_job_ltt_jobid ON job (last_transition_time, job_id) WITH (fillfactor = 80);

CREATE INDEX idx_job_active_queue_jobset ON job (queue, jobset) WITH (fillfactor = 80)
WHERE state IN (1, 2, 3, 8);

CREATE INDEX idx_job_queue_namespace ON job (queue, namespace) WITH (fillfactor = 80);

CREATE INDEX idx_job_latest_run_id ON job (latest_run_id) WITH (fillfactor = 80);

CREATE INDEX idx_job_submitted ON job (submitted DESC);

-- Remove submitted from child tables
ALTER TABLE job_run DROP COLUMN IF EXISTS submitted;

ALTER TABLE job_spec DROP COLUMN IF EXISTS submitted;

ALTER TABLE job_error DROP COLUMN IF EXISTS submitted;

COMMIT;
