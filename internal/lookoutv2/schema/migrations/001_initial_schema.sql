CREATE TABLE IF NOT EXISTS job (
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
    job_spec                     bytea         NOT NULL,
    duplicate                    bool          NOT NULL DEFAULT false,
    priority_class               varchar(63)   NULL,
    latest_run_id                varchar(36)   NULL
);
ALTER TABLE job ALTER COLUMN job_spec SET STORAGE EXTERNAL;

CREATE TABLE IF NOT EXISTS job_run (
    run_id        varchar(36)  NOT NULL PRIMARY KEY,
    job_id        varchar(32)  NOT NULL,
    cluster       varchar(512) NOT NULL,
    node          varchar(512) NULL,
    pending       timestamp    NOT NULL,
    started       timestamp    NULL,
    finished      timestamp    NULL,
    job_run_state smallint     NOT NULL,
    error         bytea        NULL,
    exit_code     int          NULL
);
ALTER TABLE job_run ALTER COLUMN error SET STORAGE EXTERNAL;

CREATE TABLE IF NOT EXISTS user_annotation_lookup (
    job_id varchar(32)   NOT NULL,
    key    varchar(1024) NOT NULL,
    value  varchar(1024) NOT NULL,
    queue  varchar(512)  NOT NULL,
    jobset varchar(1024) NOT NULL,
    PRIMARY KEY (job_id, key)
);

CREATE INDEX idx_job_queue_pattern_last_transition_time_seconds ON job (queue varchar_pattern_ops, last_transition_time_seconds);
CREATE INDEX idx_job_queue_last_transition_time_seconds ON job (queue, last_transition_time_seconds);
CREATE INDEX idx_job_queue ON  job (queue);
CREATE INDEX idx_job_jobset_last_transition_time_seconds ON job (jobset, last_transition_time_seconds);
CREATE INDEX idx_job_queue_jobset_last_transition_time_seconds ON job (queue, jobset, last_transition_time_seconds);
CREATE INDEX idx_job_queue_jobset_state ON job (queue, jobset, state);
CREATE INDEX idx_job_jobset_pattern ON job (jobset varchar_pattern_ops);
CREATE INDEX idx_job_state ON job (state);

CREATE INDEX idx_job_run_job_id ON job_run (job_id);
CREATE INDEX idx_job_run_job_id_node ON job_run (job_id, node);
CREATE INDEX idx_job_run_node ON job_run (node);

CREATE INDEX idx_user_annotation_lookup_key_value ON user_annotation_lookup (key, value);
CREATE INDEX idx_user_annotation_lookup_queue_jobset_key_value ON user_annotation_lookup (queue, jobset, key, value);
