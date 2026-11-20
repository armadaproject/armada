-- target_schema.sql
-- Target partitioned shape for the job table.
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
-- The primary key and indexes are in target_schema_indexes.sql, so that the
-- conversion can build them after copying the rows, which is much faster
-- than maintaining them row by row during the copy.

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
    preempt_user                 varchar(512)  NULL,
    reprioritize_user            varchar(512)  NULL
) PARTITION BY LIST (state);

CREATE TABLE {{TABLE}}_active PARTITION OF {{TABLE}}
    FOR VALUES IN (1, 2, 3, 8)
    WITH (fillfactor = 70);

CREATE TABLE {{TABLE}}_terminated PARTITION OF {{TABLE}}
    FOR VALUES IN (4, 5, 6, 7, 9)
    WITH (fillfactor = 70);

ALTER TABLE {{TABLE}}_active ALTER COLUMN job_spec SET STORAGE EXTERNAL;
ALTER TABLE {{TABLE}}_terminated ALTER COLUMN job_spec SET STORAGE EXTERNAL;
