CREATE TABLE IF NOT EXISTS job_metadata (
    job_id         text  NOT NULL PRIMARY KEY,
    submit_message bytea NOT NULL,
    groups         bytea
);

ALTER TABLE job_metadata ALTER COLUMN submit_message SET STORAGE EXTERNAL;
ALTER TABLE job_metadata ALTER COLUMN groups         SET STORAGE EXTERNAL;

ALTER TABLE jobs ALTER COLUMN submit_message DROP NOT NULL;
