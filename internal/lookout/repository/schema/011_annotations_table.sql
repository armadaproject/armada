CREATE TABLE user_annotation_lookup (
    job_id varchar(32)   NOT NULL,
    key    varchar(1024) NOT NULL,
    value  varchar(1024) NOT NULL,
    PRIMARY KEY (job_id, key)
);

CREATE INDEX idx_user_annotation_lookup_key_value ON user_annotation_lookup (key, value);
