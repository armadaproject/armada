CREATE TABLE IF NOT EXISTS job_deduplication
(
  deduplication_id text NOT NULL PRIMARY KEY,
  job_id text NOT NULL,
  inserted TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_job_deduplication_inserted ON job_deduplication (inserted);
