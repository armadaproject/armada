-- Backfill job_metadata from any jobs rows whose submit_message / groups still
-- live only on the jobs table. Intended to run while both services are in
-- dualWrite phase, immediately before flipping to cutover; covers pre-migration
-- rows and any row that was inserted in legacy phase and never re-upserted.
INSERT INTO job_metadata (job_id, submit_message, groups)
SELECT j.job_id, j.submit_message, j.groups
FROM jobs j
WHERE j.submit_message IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM job_metadata jm WHERE jm.job_id = j.job_id)
ON CONFLICT (job_id) DO NOTHING;
