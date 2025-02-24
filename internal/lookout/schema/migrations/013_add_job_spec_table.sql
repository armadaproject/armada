CREATE TABLE IF NOT EXISTS job_spec (
  job_id           varchar(32)   NOT NULL PRIMARY KEY,
  job_spec         bytea         NOT NULL
  );
ALTER TABLE job_spec ALTER COLUMN job_spec SET STORAGE EXTERNAL;
ALTER TABLE job ALTER COLUMN job_spec DROP NOT NULL;
