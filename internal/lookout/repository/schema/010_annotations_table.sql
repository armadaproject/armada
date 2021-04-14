CREATE TABLE job
(
    job_id           varchar(32)   NOT NULL,
    annotation_key   varchar(1024) NOT NULL,
    annotation_value varchar(1024) NOT NULL,
    PRIMARY KEY (job_id, annotation_key)
);
