-- hotcold_down.sql reverts the hot/cold table split applied by hotcold_up.sql.
--
-- It removes the job_all view, drops job_historical entirely, and removes the
-- active-state CHECK constraint from job. No data is preserved: the caller is
-- expected to truncate job_historical before running this (as TearDown does).
-- The view must be dropped before the table because it depends on job_historical.
--
-- This file is embedded by internal/broadside/db/hotcold.go and executed by
-- TearDown when the HotColdSplit feature toggle is enabled.

BEGIN;
DROP VIEW IF EXISTS job_all;
DROP TABLE IF EXISTS job_historical;
ALTER TABLE job DROP CONSTRAINT IF EXISTS chk_job_active_state;
COMMIT;
