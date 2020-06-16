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

	leased             timestamp  NULL,
	cluster            varchar(128) NULL
)