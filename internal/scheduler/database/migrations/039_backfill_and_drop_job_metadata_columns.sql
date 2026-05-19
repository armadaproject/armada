BEGIN;

LOCK TABLE jobs, job_metadata IN ACCESS EXCLUSIVE MODE;

INSERT INTO job_metadata (job_id, submit_message, groups)
SELECT j.job_id, j.submit_message, j.groups
FROM jobs j
WHERE j.submit_message IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM job_metadata jm WHERE jm.job_id = j.job_id)
ON CONFLICT (job_id) DO NOTHING;

ALTER TABLE jobs DROP COLUMN IF EXISTS submit_message;
ALTER TABLE jobs DROP COLUMN IF EXISTS groups;

COMMIT;
