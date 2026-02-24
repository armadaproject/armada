-- Add failure_count column for retry policy tracking.
-- This column is incremented only when the Retry action is applied,
-- not when Ignore action is applied.
ALTER TABLE jobs ADD COLUMN failure_count integer NOT NULL DEFAULT 0;
