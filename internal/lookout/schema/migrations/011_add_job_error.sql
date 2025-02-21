CREATE TABLE IF NOT EXISTS job_error (
  job_id                      varchar(32)   NOT NULL PRIMARY KEY,
  error         bytea         NOT NULL
);
ALTER TABLE job_error ALTER COLUMN error SET STORAGE EXTERNAL;
