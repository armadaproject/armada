CREATE TABLE jobset
(
    id        bigserial PRIMARY KEY,
    queue     varchar(512) NOT NULL,
    jobset    varchar(512) NOT NULL,
);

CREATE TABLE event
(
    jobset    bigint,
    sequence  bigint,
    msg bytea,
    PRIMARY KEY (jobset, sequence)
);