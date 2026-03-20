-- partition_up.sql converts the job table into a range-partitioned table
-- on the submitted column with daily partitions.
--
-- This file is embedded by internal/broadside/db/partition.go and executed
-- when the PartitionBySubmitted feature toggle is enabled.
--
-- This runs against an empty database (immediately after Lookout migrations).
-- Daily partition CREATE statements and the DEFAULT partition are generated
-- dynamically in Go after this static migration completes.

BEGIN;

-- Step 1: Add submitted column to child tables (nullable initially, set NOT
-- NULL immediately since tables are empty at this point)
ALTER TABLE job_run
ADD COLUMN IF NOT EXISTS submitted timestamp NOT NULL DEFAULT '1970-01-01';

ALTER TABLE job_spec
ADD COLUMN IF NOT EXISTS submitted timestamp NOT NULL DEFAULT '1970-01-01';

ALTER TABLE job_error
ADD COLUMN IF NOT EXISTS submitted timestamp NOT NULL DEFAULT '1970-01-01';

-- Remove the defaults (they were only needed for the ALTER)
ALTER TABLE job_run ALTER COLUMN submitted DROP DEFAULT;

ALTER TABLE job_spec ALTER COLUMN submitted DROP DEFAULT;

ALTER TABLE job_error ALTER COLUMN submitted DROP DEFAULT;

-- Step 2: Convert job to partitioned table
ALTER TABLE job RENAME TO job_unpartitioned;

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
    annotations                  jsonb         NOT NULL DEFAULT '{}'::jsonb,
    external_job_uri             varchar(1024) NULL,
    cancel_user                  varchar(512)  NULL,
    PRIMARY KEY (job_id, submitted)
) PARTITION BY RANGE (submitted);

-- Step 3: Drop old indexes so the names are free for the new table.
-- The old table (job_unpartitioned) is kept until Go code has created
-- partitions and moved the data across.
DROP INDEX IF EXISTS idx_job_queue;

DROP INDEX IF EXISTS idx_job_queue_pattern_last_transition_time_seconds;

DROP INDEX IF EXISTS idx_job_queue_last_transition_time_seconds;

DROP INDEX IF EXISTS idx_job_jobset_last_transition_time_seconds;

DROP INDEX IF EXISTS idx_job_queue_jobset_last_transition_time_seconds;

DROP INDEX IF EXISTS idx_job_queue_jobset_state;

DROP INDEX IF EXISTS idx_job_jobset_pattern;

DROP INDEX IF EXISTS idx_job_state;

DROP INDEX IF EXISTS idx_job_submitted;

DROP INDEX IF EXISTS idx_job_ltt_jobid;

DROP INDEX IF EXISTS idx_job_active_queue_jobset;

DROP INDEX IF EXISTS idx_job_queue_namespace;

DROP INDEX IF EXISTS idx_job_latest_run_id;

-- Step 4: Create indexes on the partitioned parent (inherited by partitions)
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

CREATE INDEX idx_job_submitted ON job (submitted);

ALTER TABLE job ALTER COLUMN job_spec SET STORAGE EXTERNAL;

COMMIT;
