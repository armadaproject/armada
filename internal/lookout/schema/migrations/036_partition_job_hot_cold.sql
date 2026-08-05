-- Partitions the job table by state into job_active and job_terminated.
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

LOCK TABLE job IN ACCESS EXCLUSIVE MODE;

CREATE TABLE job_new (
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

CREATE TABLE job_new_active PARTITION OF job_new
    FOR VALUES IN (1, 2, 3, 8)
    WITH (fillfactor = 70);

CREATE TABLE job_new_terminated PARTITION OF job_new
    FOR VALUES IN (4, 5, 6, 7, 9)
    WITH (fillfactor = 70);

ALTER TABLE job_new_active ALTER COLUMN job_spec SET STORAGE EXTERNAL;
ALTER TABLE job_new_terminated ALTER COLUMN job_spec SET STORAGE EXTERNAL;

CREATE INDEX idx_job_new_queue_last_transition_time_seconds
    ON job_new (queue, last_transition_time_seconds)
    WITH (fillfactor = 80);
CREATE INDEX idx_job_new_queue_jobset_state
    ON job_new (queue, jobset, state)
    WITH (fillfactor = 80);
CREATE INDEX idx_job_new_state
    ON job_new (state)
    WITH (fillfactor = 80);
CREATE INDEX idx_job_new_submitted
    ON job_new (submitted DESC);
CREATE INDEX idx_job_new_jobset_pattern
    ON job_new (jobset varchar_pattern_ops)
    WITH (fillfactor = 80);
CREATE INDEX idx_job_new_annotations_path
    ON job_new USING GIN (annotations jsonb_ops)
    WITH (fastupdate = true, gin_pending_list_limit = 33554432);
CREATE INDEX idx_job_new_latest_run_id
    ON job_new (latest_run_id)
    WITH (fillfactor = 80);
CREATE INDEX idx_job_new_queue_namespace
    ON job_new (queue, namespace)
    WITH (fillfactor = 80);
CREATE INDEX idx_job_new_ltt_jobid
    ON job_new (last_transition_time, job_id)
    WITH (fillfactor = 80);

CREATE INDEX idx_job_new_active_queue_jobset
    ON job_new_active (queue, jobset)
    WITH (fillfactor = 80);

INSERT INTO job_new (
    job_id, queue, owner, jobset, cpu, memory, ephemeral_storage, gpu,
    priority, submitted, cancelled, state, last_transition_time,
    last_transition_time_seconds, job_spec, duplicate, priority_class,
    latest_run_id, cancel_reason, namespace, annotations,
    external_job_uri, cancel_user
)
SELECT
    job_id, queue, owner, jobset, cpu, memory, ephemeral_storage, gpu,
    priority, submitted, cancelled, state, last_transition_time,
    last_transition_time_seconds, job_spec, duplicate, priority_class,
    latest_run_id, cancel_reason, namespace, annotations,
    external_job_uri, cancel_user
FROM job;

-- no CASCADE: loud failure for a destructive operation is safer than silently dropping dependent objects
DROP TABLE job;

ALTER TABLE job_new RENAME TO job;
ALTER TABLE job_new_active RENAME TO job_active;
ALTER TABLE job_new_terminated RENAME TO job_terminated;
ALTER INDEX job_new_pkey RENAME TO job_pkey;
ALTER INDEX idx_job_new_queue_last_transition_time_seconds RENAME TO idx_job_queue_last_transition_time_seconds;
ALTER INDEX idx_job_new_queue_jobset_state RENAME TO idx_job_queue_jobset_state;
ALTER INDEX idx_job_new_state RENAME TO idx_job_state;
ALTER INDEX idx_job_new_submitted RENAME TO idx_job_submitted;
ALTER INDEX idx_job_new_jobset_pattern RENAME TO idx_job_jobset_pattern;
ALTER INDEX idx_job_new_annotations_path RENAME TO idx_job_annotations_path;
ALTER INDEX idx_job_new_latest_run_id RENAME TO idx_job_latest_run_id;
ALTER INDEX idx_job_new_queue_namespace RENAME TO idx_job_queue_namespace;
ALTER INDEX idx_job_new_ltt_jobid RENAME TO idx_job_ltt_jobid;
ALTER INDEX idx_job_new_active_queue_jobset RENAME TO idx_job_active_queue_jobset;
