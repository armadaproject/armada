CREATE TABLE IF NOT EXISTS job_deduplication
(
  deduplication_id text NOT NULL PRIMARY KEY,
  job_id text NOT NULL
)
