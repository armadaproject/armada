ALTER TABLE runs ADD COLUMN IF NOT EXISTS failed_timestamp timestamptz NULL;
