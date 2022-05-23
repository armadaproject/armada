CREATE TABLE jobset
(
    id        bigserial PRIMARY KEY,
    queue     text NOT NULL,
    jobset    text NOT NULL,
    created   timestamp,
    UNIQUE (queue, jobset)
);
CREATE INDEX idx_jobset_created ON jobset(created);

CREATE TABLE latest_seqno
(
    jobset_id   bigint PRIMARY KEY,
    seqno       bigint NOT NULL,
    update_time timestamp
);
CREATE INDEX latest_seqno_update_time ON latest_seqno(update_time);

CREATE TABLE event
(
    jobset_id   bigint NOT NULL,
    seqno       bigint NOT NULL,
    event       bytea NOT NULL,
    PRIMARY KEY (jobset_id, seqno)
);