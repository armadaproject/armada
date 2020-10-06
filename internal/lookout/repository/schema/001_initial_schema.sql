CREATE TABLE job
(
    job_id    varchar(32)  NOT NULL PRIMARY KEY,
    queue     varchar(512) NOT NULL,
    jobset    varchar(512) NOT NULL,

    priority  float        NULL,
    submitted timestamp    NULL,
    cancelled timestamp    NULL,

    job       jsonb        NULL
);

CREATE TABLE job_run
(
    run_id    varchar(36)  NOT NULL PRIMARY KEY,
    job_id    varchar(32)  NOT NULL,

    cluster   varchar(512) NULL,
    node      varchar(512) NULL,

    created   timestamp    NULL,
    started   timestamp    NULL,
    finished  timestamp    NULL,

    succeeded bool         NULL,
    error     varchar(512) NULL
);

CREATE TABLE job_run_container
(
    run_id         varchar(32) NOT NULL,
    container_name varchar(512) NOT NULL,
    exit_code      int         NOT NULL,
    PRIMARY KEY (run_id, container_name)
)


