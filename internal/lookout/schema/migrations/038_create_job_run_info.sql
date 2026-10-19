CREATE TABLE IF NOT EXISTS job_run_info
(
  run_id varchar(36) NOT NULL PRIMARY KEY,
  start_up_info jsonb
);
