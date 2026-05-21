-- These columns have been migrated to job_metadata
ALTER TABLE jobs DROP COLUMN IF EXISTS submit_message;
ALTER TABLE jobs DROP COLUMN IF EXISTS groups;
