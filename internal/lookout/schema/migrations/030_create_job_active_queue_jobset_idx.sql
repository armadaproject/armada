-- Supports the active_job_sets subquery used when filtering by active job sets:
--
--     SELECT DISTINCT queue, jobset FROM job WHERE state IN (1, 2, 3, 8)
--
-- The existing idx_job_queue_jobset_state (queue, jobset, state) cannot serve this
-- efficiently because state is the trailing column and there is no leading predicate
-- on queue or jobset. This partial index contains only active-state rows, so it is
-- small and allows PostgreSQL to satisfy the entire subquery as an index-only scan.
-- fillfactor 80 accounts for frequent state transitions on active jobs.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_active_queue_jobset ON job (queue, jobset)
WHERE state IN (1, 2, 3, 8)
WITH (fillfactor = '80');
