-- Optimized index for cancellation queries
--
-- Context: After benchmarking, we discovered that indexing columns we UPDATE causes
-- massive write amplification. With 400k jobs, partial indexes with WHERE clauses
-- also hurt performance because PostgreSQL must evaluate the condition on every write.
--
-- This simple covering index avoids both problems:
-- 1. Only indexes search columns (queue, job_set, job_id), not updated columns
-- 2. No WHERE clause means no overhead evaluating conditions
-- 3. Stable performance regardless of job lifecycle state changes
--
-- The slightly larger index size is worth the 3x performance improvement.

CREATE INDEX CONCURRENTLY idx_jobs_queue_jobset_id ON jobs (queue, job_set, job_id);