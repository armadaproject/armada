-- Backfill job_specs from any jobs rows whose submit_message / groups still
-- live only on the jobs table. Intended to run while both services are in
-- dualWrite phase, immediately before flipping to cutover; covers pre-migration
-- rows and any row that was inserted in legacy phase and never re-upserted.
-- Legacy columns are intentionally left in place; they will be dropped in a
-- later migration once cutover has proven stable in production.
INSERT INTO job_specs (job_id, submit_message, groups)
SELECT j.job_id, j.submit_message, j.groups
FROM jobs j
WHERE j.submit_message IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM job_specs js WHERE js.job_id = j.job_id)
ON CONFLICT (job_id) DO NOTHING;
