-- hotcold_up.sql applies the hot/cold table split to the Lookout job schema.
--
-- The split separates the job table into two physical tables:
--
--   job           - active jobs only (states: 1=Queued, 2=Pending, 3=Running, 8=Leased)
--   job_historical - terminal jobs only (states: 4=Succeeded, 5=Failed, 6=Cancelled,
--                    7=Preempted, 9=Rejected)
--
-- A UNION ALL view named job_all spans both tables. Queries that target job_all can
-- rely on PostgreSQL constraint exclusion to prune the irrelevant side of the union
-- when a state filter is present, because each table carries a CHECK constraint
-- restricting it to its respective state range.
--
-- The migration is idempotent: all CREATE statements use IF NOT EXISTS and
-- ADD CONSTRAINT uses a named constraint so re-running is safe.
--
-- This file is embedded by internal/broadside/db/hotcold.go and executed when the
-- HotColdSplit feature toggle is enabled. The corresponding revert is hotcold_down.sql.

-- Step 1: create the cold table with the same schema as job, plus a CHECK constraint
-- that restricts it to terminal states. SET STORAGE EXTERNAL keeps job_spec bytes out
-- of the main relation to avoid bloating index pages.
--
-- IMPORTANT: the column order must exactly match the physical column order in job,
-- which reflects the order columns were added across all migrations:
--   001: job_id … latest_run_id
--   002: cancel_reason
--   004: namespace
--   005: annotations
--   014: external_job_uri
--   015: cancel_user
-- The UNION ALL view uses SELECT *, which matches columns positionally.
CREATE TABLE IF NOT EXISTS job_historical (
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
    cancel_user                  varchar(512)  NULL,
    CONSTRAINT chk_job_historical_terminal_state
        CHECK (state IN (4, 5, 6, 7, 9))
);
ALTER TABLE job_historical ALTER COLUMN job_spec SET STORAGE EXTERNAL;

-- Step 2: atomically move any terminal rows already in job across to job_historical.
-- COALESCE handles rows that may have NULL annotations despite the NOT NULL default.
WITH moved AS (
    DELETE FROM job
    WHERE state IN (4, 5, 6, 7, 9)
    RETURNING
        job_id, queue, owner, jobset,
        cpu, memory, ephemeral_storage, gpu, priority,
        submitted, cancelled, state,
        last_transition_time, last_transition_time_seconds,
        job_spec, duplicate, priority_class, latest_run_id,
        cancel_reason, namespace, annotations, external_job_uri, cancel_user
)
INSERT INTO job_historical (
    job_id, queue, owner, jobset,
    cpu, memory, ephemeral_storage, gpu, priority,
    submitted, cancelled, state,
    last_transition_time, last_transition_time_seconds,
    job_spec, duplicate, priority_class, latest_run_id,
    cancel_reason, namespace, annotations, external_job_uri, cancel_user
)
SELECT
    job_id, queue, owner, jobset,
    cpu, memory, ephemeral_storage, gpu, priority,
    submitted, cancelled, state,
    last_transition_time, last_transition_time_seconds,
    job_spec, duplicate, priority_class, latest_run_id,
    cancel_reason, namespace, COALESCE(annotations, '{}'::jsonb), external_job_uri, cancel_user
FROM moved;

-- Step 3: add a CHECK constraint to job restricting it to active states.
-- This is what enables constraint exclusion on job_all queries filtered by state.
-- PostgreSQL does not support ADD CONSTRAINT IF NOT EXISTS, so a DO block is used
-- to make this idempotent.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_job_active_state'
    ) THEN
        ALTER TABLE job ADD CONSTRAINT chk_job_active_state
            CHECK (state IN (1, 2, 3, 8));
    END IF;
END$$;

-- Step 4: create indexes on job_historical mirroring those on job so that
-- filtered queries against job_all perform equally well on both sides.
CREATE INDEX IF NOT EXISTS idx_job_historical_queue
    ON job_historical (queue);
CREATE INDEX IF NOT EXISTS idx_job_historical_queue_last_transition_time_seconds
    ON job_historical (queue, last_transition_time_seconds);
CREATE INDEX IF NOT EXISTS idx_job_historical_jobset_last_transition_time_seconds
    ON job_historical (jobset, last_transition_time_seconds);
CREATE INDEX IF NOT EXISTS idx_job_historical_queue_jobset_last_transition_time_seconds
    ON job_historical (queue, jobset, last_transition_time_seconds);
CREATE INDEX IF NOT EXISTS idx_job_historical_queue_jobset_state
    ON job_historical (queue, jobset, state);
CREATE INDEX IF NOT EXISTS idx_job_historical_state
    ON job_historical (state);
CREATE INDEX IF NOT EXISTS idx_job_historical_submitted
    ON job_historical (submitted);
CREATE INDEX IF NOT EXISTS idx_job_historical_ltt_jobid
    ON job_historical (last_transition_time, job_id);

-- Step 5: create the UNION ALL view that query repositories target when the
-- hot/cold split is enabled.
CREATE OR REPLACE VIEW job_all AS
    SELECT job_id, queue, owner, jobset, cpu, memory, ephemeral_storage, gpu,
           priority, submitted, cancelled, state, last_transition_time,
           last_transition_time_seconds, job_spec, duplicate, priority_class,
           latest_run_id, cancel_reason, namespace, annotations,
           external_job_uri, cancel_user
    FROM job
    UNION ALL
    SELECT job_id, queue, owner, jobset, cpu, memory, ephemeral_storage, gpu,
           priority, submitted, cancelled, state, last_transition_time,
           last_transition_time_seconds, job_spec, duplicate, priority_class,
           latest_run_id, cancel_reason, namespace, annotations,
           external_job_uri, cancel_user
    FROM job_historical;
