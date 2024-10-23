ALTER TABLE executor_settings ADD COLUMN set_by_user text;
ALTER TABLE executor_settings ADD COLUMN set_at_time timestamptz;
