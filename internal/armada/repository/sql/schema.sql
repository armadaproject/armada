CREATE TABLE job_queue
(
    id       varchar(32)  NOT NULL PRIMARY KEY,
    queue    varchar(512) NOT NULL,
    jobset   varchar(512) NOT NULL,

    priority float        NOT NULL,
    created  timestamp    NOT NULL,
    job      jsonb        NOT NULL,

    leased   timestamp    NULL,
    cluster  varchar(128) NULL
);

CREATE INDEX job_queue_queue_cluster_priority_created_index
    ON job_queue (queue, cluster, priority, created);

CREATE TABLE queue
(
    name  varchar(512) NOT NULL PRIMARY KEY,
    queue jsonb        NOT NULL
);

CREATE TABLE cluster_scheduling_info
(
    cluster varchar(512) NOT NULL PRIMARY KEY,
    data    jsonb        NOT NULL
);

CREATE TABLE cluster_usage
(
    cluster varchar(512) NOT NULL PRIMARY KEY,
    data    jsonb        NOT NULL
);

CREATE TABLE cluster_leased
(
    cluster varchar(512) NOT NULL PRIMARY KEY,
    data    jsonb        NOT NULL
);

CREATE TABLE cluster_priority
(
    cluster  varchar(512) NOT NULL,
    queue    varchar(512) NOT NULL,
    priority float        NOT NULL,
    PRIMARY KEY (cluster, queue)
);
