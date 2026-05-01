-- Initial consolidated schema for the lookouthc (hot-cold) Lookout database.
--
-- This is a fresh schema, NOT a migration of the existing Lookout database.
-- The new lookouthc stack runs on a separate database; the existing lookout
-- database is unchanged.
--
-- The job table is natively partitioned by LIST on state:
--   job_active:     states 1 (Queued), 2 (Pending), 3 (Running), 8 (Leased)
--   job_terminated: states 4 (Succeeded), 5 (Failed), 6 (Cancelled),
--                   7 (Preempted), 9 (Rejected)
--
-- PostgreSQL automatically moves rows between partitions when state is updated.
-- Partition pruning ensures queries filtered by state only scan the relevant
-- partition.
--
-- The PRIMARY KEY is (job_id, state) because PostgreSQL requires the partition
-- key in any unique constraint. The application logic enforces that job_id
-- alone is globally unique (ULIDs), though this is not enforced by the
-- database.

-- Job table: natively partitioned by state.
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

-- Store large job specs out of the main relation on each partition.
ALTER TABLE job_active ALTER COLUMN job_spec SET STORAGE EXTERNAL;
ALTER TABLE job_terminated ALTER COLUMN job_spec SET STORAGE EXTERNAL;

-- Indexes on the parent table propagate to both partitions.
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

-- Active-only index: lives on the job_active partition directly since the
-- equivalent partial index on the original unpartitioned schema filtered on
-- exactly these states. Partitioning makes the WHERE clause implicit.
CREATE INDEX idx_job_active_queue_jobset
    ON job_active (queue, jobset)
    WITH (fillfactor = 80);

-- Job run table (unpartitioned).
CREATE TABLE job_run (
    run_id              varchar(36)  NOT NULL PRIMARY KEY,
    job_id              varchar(32)  NOT NULL,
    cluster             varchar(512) NOT NULL,
    node                varchar(512) NULL,
    pending             timestamp    NULL,
    started             timestamp    NULL,
    finished            timestamp    NULL,
    job_run_state       smallint     NOT NULL,
    error               bytea        NULL,
    exit_code           int          NULL,
    leased              timestamp    NULL,
    debug               bytea        NULL,
    pool                text         NULL,
    ingress_addresses   jsonb        NULL,
    failure_category    varchar(63)  NULL,
    failure_subcategory varchar(63)  NULL
) WITH (fillfactor = 70);
ALTER TABLE job_run ALTER COLUMN error SET STORAGE EXTERNAL;

CREATE INDEX idx_job_run_job_id ON job_run (job_id) WITH (fillfactor = 80);
CREATE INDEX idx_job_run_node ON job_run (node) WITH (fillfactor = 80);
CREATE INDEX idx_job_run_state_pool ON job_run (job_run_state, pool);
CREATE INDEX idx_job_run_run_id_cluster_node
    ON job_run (run_id, cluster, node) WITH (fillfactor = 80);
CREATE INDEX idx_job_run_run_id_node
    ON job_run (run_id, node) WITH (fillfactor = 80);
CREATE INDEX idx_job_run_run_id_pool
    ON job_run (run_id, pool) WITH (fillfactor = 80);
CREATE INDEX idx_job_run_cluster_run_id
    ON job_run (cluster, run_id) WITH (fillfactor = 80);

-- Job spec (large bytea; kept out of main relation).
CREATE TABLE job_spec (
    job_id   varchar(32) NOT NULL PRIMARY KEY,
    job_spec bytea       NOT NULL
);
ALTER TABLE job_spec ALTER COLUMN job_spec SET STORAGE EXTERNAL;

-- Job error (large bytea; kept out of main relation).
CREATE TABLE job_error (
    job_id varchar(32) NOT NULL PRIMARY KEY,
    error  bytea       NOT NULL
);
ALTER TABLE job_error ALTER COLUMN error SET STORAGE EXTERNAL;

-- Queue definitions.
CREATE TABLE queue (
    name       text  NOT NULL PRIMARY KEY,
    definition bytea NOT NULL
);

-- Job deduplication tracking.
CREATE TABLE job_deduplication (
    deduplication_id text      NOT NULL PRIMARY KEY,
    job_id           text      NOT NULL,
    inserted         timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_job_deduplication_inserted ON job_deduplication (inserted);
