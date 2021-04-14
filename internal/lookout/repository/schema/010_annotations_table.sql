CREATE TABLE annotation (
    job_id varchar(32)   NOT NULL,
    key    varchar(1024) NOT NULL,
    value  varchar(1024) NOT NULL,
    PRIMARY KEY (job_id, key)
);

CREATE INDEX idx_annotation_key_value ON annotation (key, value);
